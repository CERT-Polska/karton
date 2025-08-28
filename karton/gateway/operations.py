from fastapi import WebSocket

from karton.core.__version__ import __version__
from karton.core.asyncio.backend import KartonAsyncBackend
from karton.core.backend import KartonServiceInfo, KartonBind
from .config import karton_config, gateway_config
from .errors import KartonGatewayError
from .models import HelloResponseMessage, HelloResponse, ErrorResponseMessage, \
    ErrorResponse, SuccessResponse

gateway_service_info = KartonServiceInfo(
    identity="karton.gateway", karton_version=__version__, service_version=__version__
)
gateway_backend = KartonAsyncBackend(karton_config, service_info=gateway_service_info)


class User:
    pass


class UserSession:
    def __init__(self, user: User, service_info: KartonServiceInfo):
        self.user = user
        self.service_info = service_info
        self.consumer_bind: KartonBind | None = None
        self.backend: KartonAsyncBackend | None = None


async def send_hello(websocket: WebSocket) -> None:
    hello_message = HelloResponseMessage(
        server_version=__version__, auth_required=gateway_config.auth_required
    )
    hello_response = HelloResponse(message=hello_message)
    await websocket.send_json(hello_response.model_dump(mode="json"))


async def send_error(websocket: WebSocket, error: KartonGatewayError) -> None:
    error_message = ErrorResponseMessage(code=error.code, error_message=str(error))
    error_response = ErrorResponse(message=error_message)
    await websocket.send_json(error_response.model_dump(mode="json"))


async def send_success(websocket: WebSocket) -> None:
    success_response = SuccessResponse()
    await websocket.send_json(success_response.model_dump(mode="json"))
