from fastapi import WebSocket, status
from starlette.websockets import WebSocketDisconnect, WebSocketDisconnected

from karton.core.gateway_protocol import (
    ErrorResponse,
    ErrorResponseMessage,
    ResponseType,
    SuccessResponse,
)

from .errors import KartonGatewayError


async def send_message(websocket: WebSocket, message: ResponseType) -> None:
    try:
        await websocket.send_text(message.model_dump_json())
    except RuntimeError as e:
        # When client suddenly disconnects and we find it out during
        # send_text, gunicorn.asgi worker raises RuntimeError
        # which is not correctly translated to the WebsocketDisconnect.
        # We need to do the translation on our own.
        if "WebSocket closed" in str(e):
            raise WebSocketDisconnect(code=status.WS_1006_ABNORMAL_CLOSURE) from e
        raise


async def close_websocket(
    websocket: WebSocket,
    code: int = status.WS_1000_NORMAL_CLOSURE,
    reason: str | None = None,
) -> None:
    # If websocket is already closed, websocket.close may raise
    # an exception. Unfortunately exception depends on the place
    # where we failed. In the same time: we just want to ensure
    # that websocket was closed.
    try:
        await websocket.close(code=code, reason=reason)
    except (WebSocketDisconnect, WebSocketDisconnected):
        pass
    except RuntimeError as e:
        if "WebSocket closed" not in str(e):
            raise


async def send_error(websocket: WebSocket, error: KartonGatewayError) -> None:
    error_message = ErrorResponseMessage(code=error.code, error_message=str(error))
    error_response = ErrorResponse(message=error_message)
    await send_message(websocket, error_response)


async def send_success(websocket: WebSocket) -> None:
    success_response = SuccessResponse()
    await send_message(websocket, success_response)
