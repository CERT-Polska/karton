import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from pydantic import ValidationError
from websockets.exceptions import ConnectionClosed

from karton.core.backend import KartonServiceInfo

from .auth import authorize_user, get_anonymous_user
from .backend import gateway_backend
from .config import gateway_config
from .errors import (
    BadCredentialsError,
    BadRequestError,
    InternalError,
    KartonGatewayError,
    OperationTimeoutError,
)
from .logger import set_connection_id, setup_logger
from .models import HelloRequest, Request
from .operations import (
    UserSession,
    call_request_handler,
    send_error,
    send_hello,
    send_success,
)

setup_logger()
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        await gateway_backend.connect()
        yield
    finally:
        await gateway_backend.close()


app = FastAPI(lifespan=lifespan)


async def initiate_session(websocket: WebSocket) -> UserSession:
    await send_hello(websocket)
    try:
        async with asyncio.timeout(gateway_config.auth_timeout):
            request_json = await websocket.receive_json()
            hello_request = HelloRequest.model_validate(request_json)
    except TimeoutError as exc:
        raise OperationTimeoutError("Client has not replied in required time") from exc
    except ValidationError as exc:
        raise BadRequestError("Invalid request", validation_error=exc) from exc

    if gateway_config.auth_required:
        credentials = hello_request.message.credentials
        if not credentials:
            raise BadRequestError("Missing credentials")
        user = await authorize_user(
            gateway_backend.redis, credentials.username, credentials.password
        )
        if user is None:
            logger.info(
                "Client failed to authenticate as '%s'",
                credentials.username,
            )
            raise BadCredentialsError("Wrong username or password")
    else:
        user = get_anonymous_user()

    service_info = KartonServiceInfo(
        identity=hello_request.message.identity,
        karton_version=hello_request.message.library_version,
        service_version=hello_request.message.service_version,
        secondary=hello_request.message.secondary_connection,
    )
    user_session = UserSession(
        user=user,
        service_info=service_info,
    )
    await send_success(websocket)
    return user_session


async def message_loop(websocket: WebSocket, user_session: UserSession) -> None:
    try:
        while True:
            try:
                request_json = await websocket.receive_json()
                try:
                    request = Request.model_validate(request_json)
                except ValidationError as exc:
                    raise BadRequestError(
                        "Invalid request", validation_error=exc
                    ) from exc
                await call_request_handler(websocket, request, user_session)
            except KartonGatewayError as error:
                logger.warning(
                    "Client request finished with error %s: %s",
                    error.__class__.__name__,
                    error,
                )
                await send_error(websocket, error)
    finally:
        await user_session.close()


@app.websocket("/gateway")
async def gateway_endpoint(websocket: WebSocket):
    set_connection_id()
    logger.info("Started connection")
    await websocket.accept()
    try:
        user_session = await initiate_session(websocket)
        logger.info(
            "Session created, identity: '%s', username: '%s'",
            user_session.identity,
            user_session.username,
        )
        await message_loop(websocket, user_session)
    except KartonGatewayError as error:
        logger.warning(
            "Client was disconnected with error %s: %s", error.__class__.__name__, error
        )
        await send_error(websocket, error)
        await websocket.close()
    except (WebSocketDisconnect, ConnectionClosed):
        # Client disconnected gracefully
        # ConnectionClosed is uvicorn-specific
        logger.info("Client disconnected gracefully")
    except Exception:
        logger.exception("Internal server error")
        internal_error = InternalError("Internal server error")
        await send_error(websocket, internal_error)
        await websocket.close()
