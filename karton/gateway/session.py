import asyncio
import logging
import random
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from time import time
from typing import Self

from fastapi import WebSocket
from pydantic import ValidationError

from karton.core.__version__ import __version__
from karton.core.asyncio.backend import KartonServiceInfo
from karton.core.auth.models import (
    AllowedOperation,
    EnableDebugOperation,
    VerifiedGrant,
)
from karton.core.auth.verifier import decode_auth_token
from karton.core.backend.base import make_debug_identity
from karton.core.gateway_protocol import (
    HelloRequest,
    HelloRequestMessage,
    HelloResponse,
    HelloResponseMessage,
    Request,
)

from .backend import gateway_backend
from .config import gateway_config
from .errors import (
    BadCredentialsError,
    BadRequestError,
    CredentialsExpiredError,
    KartonGatewayError,
    OperationTimeoutError,
    UnauthorizedError,
)
from .messages import close_websocket, send_error, send_message, send_success
from .operations import call_request_handler
from .shutdown import shutdown_latch

HEARTBEAT_BASE_INTERVAL = 5.0
HEARTBEAT_HARD_TIMEOUT = 15
INIT_SESSION_TIMEOUT = 30.0
IDLE_TIMEOUT = 30.0

logger = logging.getLogger(__name__)


def parse_auth_tokens(client_tokens: list[str]) -> list[VerifiedGrant]:
    """
    Parse and validate set of authorization tokens provided by client.

    Returns list of VerifiedGrant objects for each valid token. This function
    doesn't fail when any of the tokens is invalid - it is just not
    included in the returned list.

    Validation in this method checks whether token itself is correct and trustworthy.
    Code that is using this method must still validate if claims in the token
    are valid in a current context e.g. whether grant applies to the identity
    provided by the client.

    :param client_tokens: List of client-provided tokens
    :return: List of VerifiedGrant objects for each valid token.
    """
    verified_grants = []
    jwks = gateway_config.jwks
    if not jwks:
        raise RuntimeError("Bug: JWKS should not be empty here")
    for client_token in client_tokens:
        try:
            verified_grant = decode_auth_token(client_token, jwks)
            verified_grants.append(verified_grant)
        except Exception as e:
            logger.warning("Client presented invalid token: %r", e)
    return verified_grants


class ClientSession:
    def __init__(
        self,
        service_info: KartonServiceInfo,
        close_on_idle: bool,
        auth_required: bool,
        auth_grants: list[VerifiedGrant],
    ) -> None:
        self.service_info = service_info
        self.close_on_idle = close_on_idle
        self.auth_required = auth_required
        self.auth_grants = auth_grants

    @property
    def identity(self) -> str:
        """
        Session identity (used as an audience for tokens)
        """
        return self.service_info.identity

    async def _maintain_heartbeat(self, connection_id: str) -> None:
        try:
            while True:
                await gateway_backend.heartbeat_service(
                    self.service_info,
                    connection_id,
                    expires_after=HEARTBEAT_HARD_TIMEOUT,
                )
                # Added random.random() to better distribute the heartbeat
                # for services that initiated connection from the start
                await asyncio.sleep(HEARTBEAT_BASE_INTERVAL + random.random())
        finally:
            await gateway_backend.unregister_service(self.service_info, connection_id)

    def can_perform(self, operation: AllowedOperation) -> bool:
        if not self.auth_required:
            return True
        now = time()
        self.auth_grants = [
            grant
            for grant in self.auth_grants
            if grant.expires_at is None or now < grant.expires_at
        ]
        if not self.auth_grants:
            raise CredentialsExpiredError("No fresh credentials available")

        for grant in self.auth_grants:
            for op in grant.claims.allowed_operations:
                if op.covers(operation):
                    return True
        raise UnauthorizedError(f"Operation '{operation.operation}' is not authorized")

    @staticmethod
    def is_grant_valid(
        grant: VerifiedGrant, hello_message: HelloRequestMessage
    ) -> bool:
        if hello_message.debug:
            for op in grant.claims.allowed_operations:
                if op.covers(EnableDebugOperation()):
                    break
            else:
                logger.warning(
                    "Client tried to enable debug mode without proper authorization"
                )
                return False
            grant_identity = make_debug_identity(
                grant.claims.identity, hello_message.instance_id
            )
        else:
            grant_identity = grant.claims.identity
        return grant_identity == hello_message.identity

    @classmethod
    @asynccontextmanager
    async def initiate_session(
        cls,
        websocket: WebSocket,
        connection_id: str,
        timeout: float = INIT_SESSION_TIMEOUT,
    ) -> AsyncIterator[Self]:
        hello_message = HelloResponseMessage(server_version=__version__)
        hello_response = HelloResponse(message=hello_message)
        await send_message(websocket, hello_response)

        try:
            async with asyncio.timeout(timeout):
                request_json = await websocket.receive_text()
                hello_request = HelloRequest.model_validate_json(request_json)
                if gateway_config.auth_required:
                    valid_auth_grants = [
                        grant
                        for grant in parse_auth_tokens(
                            hello_request.message.auth_tokens
                        )
                        if ClientSession.is_grant_valid(grant, hello_request.message)
                    ]
                    if not valid_auth_grants:
                        raise BadCredentialsError(
                            "Client has not provided valid authorization tokens "
                            "for declared identity"
                        )
                else:
                    valid_auth_grants = []
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
            debug=hello_request.message.debug,
        )
        session = cls(
            service_info=service_info,
            close_on_idle=hello_request.message.close_on_idle,
            auth_required=gateway_config.auth_required,
            auth_grants=valid_auth_grants,
        )
        await gateway_backend.register_service(
            service_info, connection_id, HEARTBEAT_HARD_TIMEOUT
        )
        try:
            async with asyncio.TaskGroup() as tg:
                heartbeat = tg.create_task(session._maintain_heartbeat(connection_id))
                try:
                    await send_success(websocket)
                    yield session
                finally:
                    heartbeat.cancel()
        except BaseExceptionGroup as excg:
            if len(excg.exceptions) == 1:
                # If there is a single exception, unwrap it
                raise excg.exceptions[0] from None
            else:
                # If both session and heartbeat failed, then
                # something serious happened, it's better to
                # bubble-up the whole group and report it
                raise

    async def message_loop(self, websocket: WebSocket):
        while True:
            if not self.close_on_idle:
                # Consumer loop connections are not closed on idle
                # Active connection maintains heartbeat, which is crucial
                # for not GC'ing the non-persistent queues too early
                request_json = await websocket.receive_text()
            else:
                try:
                    async with asyncio.timeout(IDLE_TIMEOUT):
                        request_json = await websocket.receive_text()
                except TimeoutError:
                    logger.info(
                        "Connection was idle for %d seconds. Closing.",
                        IDLE_TIMEOUT,
                    )
                    await close_websocket(websocket, reason="Connection was idle")
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
