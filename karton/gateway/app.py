import asyncio
import dataclasses
import enum
import logging
from contextlib import asynccontextmanager
from typing import Any, Awaitable, Callable, Type, TypeVar

from fastapi import FastAPI, WebSocket
from pydantic import ValidationError

from karton.core.__version__ import __version__
from karton.core.asyncio.backend import (
    KartonAsyncBackend,
    KartonBind,
    KartonServiceInfo,
)
from karton.core.config import Config
from karton.core.task import Task, TaskPriority

from .auth import User, authorize_user
from .errors import (
    AlreadyBoundError,
    AuthTimeoutError,
    BadRequestError,
    InternalError,
    InvalidBindError,
    KartonGatewayError,
    KartonGatewayTaskError,
)
from .models import (
    AuthRequestMessage,
    BindRequest,
    ConsumerBindRequestMessage,
    DeclareTaskRequest,
    ErrorResponse,
    ErrorResponseMessage,
    GetTaskRequest,
    HelloResponse,
    HelloResponseMessage,
    LogConsumerBindRequestMessage,
    ProducerBindRequestMessage,
    Request,
    ResourceUploadUrl,
    SendTaskRequest,
    SetTaskStatusRequest,
    SuccessResponse,
    TaskDeclaredResponse,
    TaskDeclaredResponseMessage,
)
from .task import parse_task_token

config = Config()
gateway_service_info = KartonServiceInfo(
    identity="karton.gateway", karton_version=__version__, service_version=__version__
)
gateway_backend = KartonAsyncBackend(config, service_info=gateway_service_info)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

AUTH_TIMEOUT = 10
GATEWAY_SERVER_SECRET_KEY = "secretkey"


@asynccontextmanager
async def lifespan(app: FastAPI):
    await gateway_backend.connect()
    yield


app = FastAPI(lifespan=lifespan)


async def send_hello(websocket: WebSocket) -> None:
    hello_message = HelloResponseMessage(server_version=__version__)
    hello_response = HelloResponse(message=hello_message)
    await websocket.send_json(hello_response.model_dump(mode="json"))


async def send_error(websocket: WebSocket, error: KartonGatewayError) -> None:
    error_message = ErrorResponseMessage(
        code=error.code, error_message=error.message, details=error.details
    )
    error_response = ErrorResponse(message=error_message)
    await websocket.send_json(error_response.model_dump(mode="json"))


async def send_success(websocket: WebSocket) -> None:
    success_response = SuccessResponse()
    await websocket.send_json(success_response.model_dump(mode="json"))


async def authorize(websocket: WebSocket, auth_timeout: int) -> User:
    try:
        with asyncio.timeout(auth_timeout):
            await send_hello(websocket)
            request_json = await websocket.receive_json()
            auth_request = AuthRequestMessage.model_validate(request_json)

        return await authorize_user(
            gateway_backend.redis, auth_request.username, auth_request.password
        )
    except TimeoutError:
        raise AuthTimeoutError()
    except ValidationError as exc:
        raise BadRequestError(details=exc.errors())


class KartonServiceType(enum.Enum):
    consumer = "consumer"
    producer = "producer"
    log_consumer = "log_consumer"


@dataclasses.dataclass
class ServiceBind:
    service_type: KartonServiceType
    service_info: KartonServiceInfo | None = None
    karton_bind: KartonBind | None = None


@dataclasses.dataclass
class UserSession:
    user: User
    service_bind: ServiceBind | None = None
    backend: KartonAsyncBackend | None = None


async def get_service_backend(
    session: UserSession, service_info: KartonServiceInfo | None = None
) -> KartonAsyncBackend:
    if session.backend and (session.backend.service_info or not service_info):
        return session.backend
    service_backend = KartonAsyncBackend(config, service_info=service_info)
    await service_backend.connect(single_connection_client=True)
    return service_backend


async def handle_bind_request(
    websocket: WebSocket, request: BindRequest, session: UserSession
) -> UserSession:
    if session.service_bind is not None:
        raise AlreadyBoundError()

    if request.message.service_type == KartonServiceType.consumer.value:
        consumer_bind_request: ConsumerBindRequestMessage = request.message
        service_bind = ServiceBind(
            service_type=KartonServiceType.consumer,
            service_info=KartonServiceInfo(
                identity=consumer_bind_request.identity,
                karton_version=consumer_bind_request.library_version,
                service_version=consumer_bind_request.service_version,
            ),
            karton_bind=KartonBind(
                identity=consumer_bind_request.identity,
                info=consumer_bind_request.info,
                version=consumer_bind_request.library_version,
                persistent=consumer_bind_request.persistent,
                filters=consumer_bind_request.filters,
                service_version=consumer_bind_request.service_version,
                is_async=consumer_bind_request.is_async,
            ),
        )
    elif request.message.service_type == KartonServiceType.producer.value:
        producer_bind_request: ProducerBindRequestMessage = request.message
        service_bind = ServiceBind(
            service_type=KartonServiceType.producer,
            service_info=KartonServiceInfo(
                identity=producer_bind_request.identity,
                karton_version=producer_bind_request.library_version,
                service_version=producer_bind_request.service_version,
            ),
        )
    elif request.message.service_type == KartonServiceType.log_consumer.value:
        log_consumer_bind_request: LogConsumerBindRequestMessage = request.message
        service_bind = ServiceBind(
            service_type=KartonServiceType.log_consumer,
            service_info=KartonServiceInfo(
                identity=log_consumer_bind_request.identity,
                karton_version=log_consumer_bind_request.library_version,
                service_version=log_consumer_bind_request.service_version,
            ),
        )
    else:
        raise RuntimeError("Unknown service type, we should never get here")

    service_backend = await get_service_backend(
        session, service_info=service_bind.service_info
    )
    if service_bind.karton_bind:
        await service_backend.register_bind(service_bind.karton_bind)

    await send_success(websocket)
    return UserSession(
        user=session.user, service_bind=service_bind, backend=service_backend
    )


async def generate_resource_upload_links(task: Task) -> list[ResourceUploadUrl]:
    ...
    return []


async def handle_declare_task_request(
    websocket: WebSocket, request: DeclareTaskRequest, session: UserSession
) -> UserSession:
    parent_token = request.message.token
    task_params = request.message.task

    if parent_token is None:
        if not session.service_bind or not session.backend:
            raise InvalidBindError()
        if session.service_bind.service_type is not KartonServiceType.producer:
            raise InvalidBindError()
        parent_task_uid = None
        parent_resources = []
    else:
        # This method can be called without service bind if user provides a token
        parent_task_info = parse_task_token(
            parent_token, GATEWAY_SERVER_SECRET_KEY, session.user.username
        )
        parent_task_uid = parent_task_info.task_uid
        parent_resources = parent_task_info.resources

    task = Task(
        headers=task_params.headers,
        payload=task_params.payload,
        headers_persistent=task_params.headers_persistent,
        payload_persistent=task_params.payload_persistent,
        priority=TaskPriority[task_params.priority],
        parent_uid=parent_task_uid,
    )

    resources = await generate_resource_upload_links(task)
    task_token = make_task_token(task, resources)

    service_backend = await get_service_backend(session)
    await service_backend.register_task(task)
    task_declared_message = TaskDeclaredResponseMessage(
        uid=task.uid,
        resources=resources,
        token=task_token,
    )
    task_declared_request = TaskDeclaredResponse(message=task_declared_message)
    await websocket.send_json(task_declared_request.model_dump(mode="json"))

    return UserSession(
        user=session.user, service_bind=session.service_bind, backend=service_backend
    )


async def handle_send_task_request(
    websocket: WebSocket, request: SendTaskRequest, session: UserSession
) -> UserSession:
    """
    TODO: That part is easy.
          We parse and validate the token
          We check if task still exists
          and then we produce unrouted task
    """
    ...


async def handle_set_task_status_request(
    websocket: WebSocket, request: SetTaskStatusRequest, session: UserSession
) -> UserSession:
    """
    TODO: That part is also easy.
          We parse and validate the token
          We get the task and check if transition is valid
          and then we set the task back
    """


async def handle_get_task_request(
    websocket: WebSocket, request: GetTaskRequest, session: UserSession
) -> UserSession: ...


TRequest = TypeVar("TRequest", bound=Request)
RequestHandler = Callable[[WebSocket, TRequest, UserSession], Awaitable[UserSession]]

REQUEST_HANDLERS: dict[Type, RequestHandler] = {
    BindRequest: handle_bind_request,
    DeclareTaskRequest: handle_declare_task_request,
    SendTaskRequest: handle_send_task_request,
    SetTaskStatusRequest: handle_set_task_status_request,
    GetTaskRequest: handle_get_task_request,
}


async def message_loop(websocket: WebSocket, user: User) -> None:
    session = UserSession(user)
    while True:
        request_json = await websocket.receive_json()
        try:
            request = Request.model_validate(request_json)
        except ValidationError as exc:
            raise BadRequestError(details=exc.errors())
        try:
            session = await REQUEST_HANDLERS[type(request)](websocket, request, session)
        except KartonGatewayTaskError as error:
            await send_error(websocket, error)


@app.websocket("/gateway")
async def gateway_endpoint(websocket: WebSocket):
    await websocket.accept()

    try:
        user = await authorize(websocket, auth_timeout=AUTH_TIMEOUT)
        await message_loop(websocket, user)
    except KartonGatewayError as error:
        await send_error(websocket, error)
    except Exception as e:
        logger.exception(e)
        internal_error = InternalError()
        await send_error(websocket, internal_error)
