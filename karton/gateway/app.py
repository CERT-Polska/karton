import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from pydantic import ValidationError
from websockets import ConnectionClosed

from .auth import User, authorize_user, get_anonymous_user
from .config import gateway_config
from .errors import (
    AuthTimeoutError,
    BadCredentialsError,
    BadRequestError,
    InternalError,
    KartonGatewayError,
    KartonGatewayTaskError,
)
from .models import AuthRequestMessage, Request
from .operations import (
    UserSession,
    call_request_handler,
    gateway_backend,
    send_error,
    send_hello,
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

AUTH_TIMEOUT = 10
GATEWAY_SERVER_SECRET_KEY = "secretkey"


@asynccontextmanager
async def lifespan(app: FastAPI):
    await gateway_backend.connect()
    yield


app = FastAPI(lifespan=lifespan)


async def authorize(websocket: WebSocket) -> User:
    try:
        async with asyncio.timeout(gateway_config.auth_timeout):
            request_json = await websocket.receive_json()
            auth_request = AuthRequestMessage.model_validate(request_json)
    except TimeoutError:
        raise AuthTimeoutError("Client has not authenticated in required time")
    except ValidationError as exc:
        raise BadRequestError(f"Invalid authentication request: {str(exc)}")

    user = await authorize_user(
        gateway_backend.redis, auth_request.username, auth_request.password
    )
    if user is None:
        raise BadCredentialsError("Wrong username or password")
    return user


async def message_loop(websocket: WebSocket, user: User) -> None:
    session = UserSession(user)
    try:
        while True:
            request_json = await websocket.receive_json()
            try:
                request = Request.model_validate(request_json)
            except ValidationError as exc:
                raise BadRequestError(f"Invalid request: {str(exc)}")
            try:
                await call_request_handler(websocket, request, session)
            except KartonGatewayTaskError as error:
                await send_error(websocket, error)
    finally:
        await session.close()


@app.websocket("/gateway")
async def gateway_endpoint(websocket: WebSocket):
    await websocket.accept()

    try:
        await send_hello(websocket)
        if gateway_config.auth_required:
            user = await authorize(websocket)
        else:
            user = get_anonymous_user()
        await message_loop(websocket, user)
    except KartonGatewayError as error:
        await send_error(websocket, error)
        await websocket.close()
    except (WebSocketDisconnect, ConnectionClosed):
        logger.info("Client disconnected")
    except Exception as e:
        logger.exception(e)
        internal_error = InternalError("Internal server error")
        await send_error(websocket, internal_error)
        await websocket.close()
