from fastapi import WebSocket

from .errors import KartonGatewayError
from .models import ErrorResponse, ErrorResponseMessage, SuccessResponse


async def send_error(websocket: WebSocket, error: KartonGatewayError) -> None:
    error_message = ErrorResponseMessage(code=error.code, error_message=str(error))
    error_response = ErrorResponse(message=error_message)
    await websocket.send_json(error_response.model_dump(mode="json"))


async def send_success(websocket: WebSocket) -> None:
    success_response = SuccessResponse()
    await websocket.send_json(success_response.model_dump(mode="json"))
