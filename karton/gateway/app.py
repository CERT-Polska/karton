import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from websockets import ConnectionClosed

from .errors import KartonGatewayError, InternalError
from .operations import gateway_backend, send_error

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        await gateway_backend.connect()
        yield
    finally:
        await gateway_backend.close()

app = FastAPI(lifespan=lifespan)

@app.websocket("/gateway")
async def gateway_endpoint(websocket: WebSocket):
    await websocket.accept()

    try:
        ...
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
