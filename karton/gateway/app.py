import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, status

from .backend import gateway_backend
from .errors import InternalError, KartonGatewayError, ShutdownError
from .logger import set_connection_id, setup_logger
from .messages import close_websocket, send_error
from .rest.routes import rest_api_router, varz_router
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

app.include_router(rest_api_router)
app.include_router(varz_router)


async def try_send_error(websocket: WebSocket, error: KartonGatewayError):
    try:
        await send_error(websocket, error)
    except WebSocketDisconnect:
        logger.warning("Client disconnected before error could be sent")


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
                "service_version=%s, instance_id=%s, close_on_idle=%s)",
                client_session.service_info.identity,
                client_session.service_info.karton_version,
                client_session.service_info.service_version,
                client_session.service_info.instance_id,
                client_session.close_on_idle,
            )
            await client_session.message_loop(websocket)
    except KartonGatewayError as error:
        logger.warning(
            "Client was disconnected with error %s: %s", error.__class__.__name__, error
        )
        await try_send_error(websocket, error)
        if isinstance(error, ShutdownError):
            await close_websocket(
                websocket, code=status.WS_1001_GOING_AWAY, reason="Server shutting down"
            )
        else:
            await close_websocket(websocket)
    except WebSocketDisconnect:
        logger.info("Client disconnected gracefully")
    except Exception:
        logger.exception("Internal server error")
        internal_error = InternalError("Internal server error")
        await try_send_error(websocket, internal_error)
