import asyncio
import logging
import random
import secrets
from contextlib import asynccontextmanager

from fastapi import WebSocket
from pydantic import ValidationError

from karton.core.__version__ import __version__
from karton.core.asyncio.backend import KartonServiceInfo
from karton.core.asyncio.backend.direct import KartonAsyncGatewayClientBackend

from .backend import gateway_backend
from .config import gateway_config
from .errors import (
    BadCredentialsError,
    BadRequestError,
    KartonGatewayError,
    OperationTimeoutError,
)
from .messages import send_error, send_success
from .models import HelloRequest, HelloResponse, HelloResponseMessage, Request
from .operations import call_request_handler
from .shutdown import shutdown_latch

HEARTBEAT_BASE_INTERVAL = 5.0
HEARTBEAT_HARD_TIMEOUT = 15
INIT_SESSION_TIMEOUT = 30.0
IDLE_SECONDARY_TIMEOUT = 30.0

logger = logging.getLogger(__name__)


class ClientSession:
    def __init__(
        self,
        service_info: KartonServiceInfo,
        service_backend: KartonAsyncGatewayClientBackend,
    ):
        self.service_info = service_info
        self.service_backend: KartonAsyncGatewayClientBackend = service_backend

    @property
    def is_bound(self) -> bool:
        return self.service_backend.karton_bind is not None

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
                if gateway_config.password is not None and (
                    not hello_request.message.password
                    or not secrets.compare_digest(
                        hello_request.message.password, gateway_config.password
                    )
                ):
                    raise BadCredentialsError("Client has provided wrong password")
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
        service_backend = KartonAsyncGatewayClientBackend(
            service_info=service_info, gateway_backend=gateway_backend
        )

        session = cls(
            service_info=service_info,
            service_backend=service_backend,
        )
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
            if self.is_bound:
                # Consumer loop connections are not closed on idle
                # Active connection maintains heartbeat, which is crucial
                # for not GC'ing the non-persistent queues too early
                request_json = await websocket.receive_text()
            else:
                try:
                    async with asyncio.timeout(IDLE_SECONDARY_TIMEOUT):
                        request_json = await websocket.receive_text()
                except TimeoutError:
                    logger.info(
                        "Connection was idle for %d seconds. Closing.",
                        IDLE_SECONDARY_TIMEOUT,
                    )
                    await websocket.close(reason="Connection was idle")
                    break

            with shutdown_latch:
                try:
                    request = Request.model_validate_json(request_json)
                except ValidationError as exc:
                    raise BadRequestError(
                        "Invalid request", validation_error=exc
                    ) from exc
                try:
                    await call_request_handler(websocket, request, session=self)
                except KartonGatewayError as error:
                    await send_error(websocket, error)
