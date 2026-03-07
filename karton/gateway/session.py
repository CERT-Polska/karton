import asyncio
import random
from contextlib import asynccontextmanager

from fastapi import WebSocket
from pydantic import ValidationError

from karton.core.__version__ import __version__
from karton.core.asyncio.backend import KartonBind, KartonServiceInfo

from .backend import gateway_backend
from .errors import BadRequestError, KartonGatewayError, OperationTimeoutError
from .messages import send_error, send_success
from .models import HelloRequest, HelloResponse, HelloResponseMessage, Request
from .operations import call_request_handler
from .shutdown import shutdown_latch

HEARTBEAT_BASE_INTERVAL = 5.0
HEARTBEAT_HARD_TIMEOUT = 15
INIT_SESSION_TIMEOUT = 30.0


class ClientSession:
    def __init__(self, service_info: KartonServiceInfo):
        self.service_info = service_info
        self.karton_bind: KartonBind | None = None

    @property
    def is_bound(self) -> bool:
        return self.karton_bind is not None

    @property
    def identity(self) -> str:
        """
        Session identity (used as an audience for tokens)
        """
        return self.service_info.identity

    async def _maintain_heartbeat(self, connection_id: str):
        while True:
            await gateway_backend.heartbeat_service(
                self.service_info, connection_id, expires_after=HEARTBEAT_HARD_TIMEOUT
            )
            # Added random.random() to better distribute the heartbeat
            # for services that initiated connection from the start
            await asyncio.sleep(HEARTBEAT_BASE_INTERVAL + random.random())

    @classmethod
    @asynccontextmanager
    async def initiate_session(
        cls,
        websocket: WebSocket,
        connection_id: str,
        timeout: float = INIT_SESSION_TIMEOUT,
    ):
        hello_message = HelloResponseMessage(server_version=__version__)
        hello_response = HelloResponse(message=hello_message)
        await websocket.send_text(hello_response.model_dump_json())

        try:
            async with asyncio.timeout(timeout):
                request_json = await websocket.receive_text()
                hello_request = HelloRequest.model_validate_json(request_json)
        except TimeoutError as exc:
            raise OperationTimeoutError(
                "Client has not replied in required time"
            ) from exc
        except ValidationError as exc:
            raise BadRequestError("Invalid request", validation_error=exc) from exc

        service_info = KartonServiceInfo(
            identity=hello_request.message.identity,
            karton_version=hello_request.message.library_version,
            service_version=hello_request.message.service_version,
            instance_id=hello_request.message.instance_id,
        )

        session = cls(service_info=service_info)
        await gateway_backend.register_service(
            service_info, connection_id, HEARTBEAT_HARD_TIMEOUT
        )
        heartbeat = asyncio.create_task(session._maintain_heartbeat(connection_id))
        await send_success(websocket)
        try:
            yield session
        finally:
            heartbeat.cancel()
            await asyncio.wait([heartbeat])
            await gateway_backend.unregister_service(service_info, connection_id)

    async def message_loop(self, websocket: WebSocket):
        while True:
            request_json = await websocket.receive_text()
            try:
                request = Request.model_validate_json(request_json)
            except ValidationError as exc:
                raise BadRequestError("Invalid request", validation_error=exc) from exc

            with shutdown_latch:
                try:
                    await call_request_handler(websocket, request, session=self)
                except KartonGatewayError as error:
                    await send_error(websocket, error)
