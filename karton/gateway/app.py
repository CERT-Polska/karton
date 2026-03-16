import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from websockets.exceptions import ConnectionClosed

from .backend import gateway_backend
from .errors import InternalError, KartonGatewayError, ShutdownError
from .logger import set_connection_id, setup_logger
from .messages import send_error
from .session import ClientSession


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        await gateway_backend.connect()
        yield
    finally:
        await gateway_backend.close()


setup_logger()
logger = logging.getLogger(__name__)
app = FastAPI(lifespan=lifespan)


@app.websocket("/gateway")
async def gateway_endpoint(websocket: WebSocket):
    connection_id = set_connection_id()
    logger.info(
        "Started connection from %s", websocket.client and websocket.client.host
    )
    await websocket.accept()

    try:
        async with ClientSession.initiate_session(
            websocket, connection_id
        ) as client_session:
            logger.info(
                "Session created for %s (karton_version=%s, "
                "service_version=%s, instance_id=%s, secondary_connection=%s)",
                client_session.service_info.identity,
                client_session.service_info.karton_version,
                client_session.service_info.service_version,
                client_session.service_info.instance_id,
                client_session.secondary_connection,
            )
            await client_session.message_loop(websocket)
    except KartonGatewayError as error:
        logger.warning(
            "Client was disconnected with error %s: %s", error.__class__.__name__, error
        )
        await send_error(websocket, error)
        if isinstance(error, ShutdownError):
            await websocket.close(code=1001, reason="Server shutting down")
        else:
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
