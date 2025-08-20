import dataclasses
import enum
import sys
import traceback
from typing import Any, Awaitable, Callable, Type, TypeVar

from fastapi import WebSocket
from pydantic import ValidationError

from karton.core.__version__ import __version__
from karton.core.asyncio.backend import (
    KartonAsyncBackend,
    KartonBind,
    KartonServiceInfo,
)
from karton.core.task import Task, TaskState

from ..core.resource import ResourceBase
from .auth import User
from .config import gateway_config, karton_config
from .errors import (
    AlreadyBoundError,
    InvalidBindError,
    InvalidTaskError,
    KartonGatewayError,
    NoTaskError,
)
from .models import (
    BindRequest,
    ConsumerBindRequestMessage,
    DeclaredResourceSpec,
    DeclareTaskRequest,
    ErrorResponse,
    ErrorResponseMessage,
    GetTaskRequest,
    HelloResponse,
    HelloResponseMessage,
    IncomingTask,
    LogConsumerBindRequestMessage,
    ProducerBindRequestMessage,
    Request,
    ResourceDownloadUrl,
    ResourceUploadUrl,
    SendTaskRequest,
    SetTaskStatusRequest,
    SuccessResponse,
    TaskDeclaredResponse,
    TaskDeclaredResponseMessage,
    TaskResponse,
    TaskResponseMessage,
)
from .task import TaskTokenInfo, make_task_token, map_resources, parse_task_token

gateway_service_info = KartonServiceInfo(
    identity="karton.gateway", karton_version=__version__, service_version=__version__
)
gateway_backend = KartonAsyncBackend(karton_config, service_info=gateway_service_info)


class KartonServiceType(enum.Enum):
    consumer = "consumer"
    producer = "producer"
    log_consumer = "log_consumer"


@dataclasses.dataclass
class ServiceBind:
    service_type: KartonServiceType
    service_info: KartonServiceInfo
    karton_bind: KartonBind | None = None


class UserSession:
    def __init__(self, user: User) -> None:
        self.user = user
        self.service_bind: ServiceBind | None = None
        self.backend: KartonAsyncBackend | None = None

    async def _get_service_backend(
        self, _service_info: KartonServiceInfo | None = None
    ) -> KartonAsyncBackend:
        if self.backend and (self.backend.service_info or not _service_info):
            return self.backend
        service_backend = KartonAsyncBackend(karton_config, service_info=_service_info)
        await service_backend.connect(single_connection_client=True)
        self.backend = service_backend
        return self.backend

    async def get_service_backend(self):
        """
        Connects to backend within a session if needed and returns the backend object
        """
        return await self._get_service_backend()

    async def get_bound_service_backend(
        self, service_bind: ServiceBind
    ) -> KartonAsyncBackend:
        """
        Connects to backend within a session and sets a service bind.
        Returns the backend object.
        """
        self.service_bind = service_bind
        return await self._get_service_backend(self.service_bind.service_info)

    def is_bound(self) -> bool:
        return self.service_bind is not None

    def get_service_type(self) -> KartonServiceType | None:
        if self.service_bind is None:
            return None
        return self.service_bind.service_type

    def get_identity(self) -> str:
        if not self.service_bind:
            # This should never happen, it's a type guard
            raise ValueError("Session is not bound to a service")
        return self.service_bind.service_info.identity

    async def close(self) -> None:
        if self.backend is not None:
            await self.backend.close()
        self.service_bind = None
        self.backend = None


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


async def handle_bind_request(
    websocket: WebSocket, request: BindRequest, session: UserSession
) -> None:
    if session.is_bound():
        raise AlreadyBoundError("Client is already bound")

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

    service_backend = await session.get_bound_service_backend(service_bind)
    if service_bind.karton_bind:
        await service_backend.register_bind(service_bind.karton_bind)

    await send_success(websocket)


PayloadBags = tuple[dict[str, Any], dict[str, Any]]


def process_declared_task_resources(
    payload_bags: PayloadBags, allowed_parent_resources: list[str], target_bucket: str
) -> tuple[PayloadBags, list[DeclaredResourceSpec]]:
    resources: dict[str, DeclaredResourceSpec] = {}

    def process_resource(resource_data: dict[str, Any]) -> Any:
        try:
            resource_spec = DeclaredResourceSpec.model_validate(resource_data)
        except ValidationError as e:
            raise InvalidTaskError(f"Invalid resource specification {str(e)}")
        if (
            not resource_spec.to_upload
            and resource_spec.uid not in allowed_parent_resources
        ):
            raise InvalidTaskError(
                f"Service is not allowed to reference resource '{resource_spec.uid}'"
            )
        if resource_spec.uid in resources:
            resources[resource_spec.uid] = resource_spec
        return ResourceBase(
            _uid=resource_spec.uid,
            _size=resource_spec.size,
            name=resource_spec.name,
            bucket=target_bucket,
            metadata=resource_spec.metadata,
            sha256=resource_spec.sha256,
        )

    transformed_payload_bags = map_resources(payload_bags, process_resource)
    return transformed_payload_bags, list(resources.values())


async def generate_resource_upload_urls(
    resources: list[DeclaredResourceSpec],
    service_backend: KartonAsyncBackend,
    target_bucket: str,
) -> list[ResourceUploadUrl]:
    upload_urls: list[ResourceUploadUrl] = []
    for resource in resources:
        if resource.to_upload:
            upload_url = await service_backend.get_presigned_object_upload_url(
                bucket=target_bucket, object_uid=resource.uid
            )
            upload_urls.append(
                ResourceUploadUrl(
                    uid=resource.uid,
                    upload_url=upload_url,
                )
            )
    return upload_urls


async def handle_declare_task_request(
    websocket: WebSocket, request: DeclareTaskRequest, session: UserSession
) -> None:
    parent_token = request.message.token
    task_params = request.message.task

    if parent_token is None:
        if session.get_service_type() is not KartonServiceType.producer:
            raise InvalidBindError("Only producers can declare root tasks")
        parent_task_uid = None
        allowed_parent_resources = []
    else:
        # This method can be called without service bind if user provides a token
        parent_task_info = parse_task_token(
            parent_token, gateway_config.secret_key, session.user.username
        )
        parent_task_uid = parent_task_info.task_uid
        allowed_parent_resources = parent_task_info.resources

    service_backend = await session.get_service_backend()

    target_bucket = service_backend.default_bucket_name
    payload_bags = (task_params.payload, task_params.payload_persistent)
    task_payload_bags, resources = process_declared_task_resources(
        payload_bags, allowed_parent_resources, target_bucket
    )
    resource_urls = await generate_resource_upload_urls(
        resources, service_backend, target_bucket
    )
    task_payload, task_payload_persistent = task_payload_bags

    # Now, we need to translate DeclaredResourceSpec to RemoteResource
    task = Task(
        headers=task_params.headers,
        headers_persistent=task_params.headers_persistent,
        payload=task_payload,
        payload_persistent=task_payload_persistent,
        priority=task_params.priority,
        parent_uid=parent_task_uid,
    )
    task_token_info = TaskTokenInfo(
        task_uid=task.uid,
        resources=[r.uid for r in resources],
    )
    task_token = make_task_token(
        task_token_info=task_token_info,
        secret_key=gateway_config.secret_key,
        username=session.user.username,
    )

    await service_backend.register_task(task)
    task_declared_message = TaskDeclaredResponseMessage(
        uid=task.uid,
        resources_to_upload=resource_urls,
        token=task_token,
    )
    task_declared_request = TaskDeclaredResponse(message=task_declared_message)
    await websocket.send_json(task_declared_request.model_dump(mode="json"))


async def handle_send_task_request(
    websocket: WebSocket, request: SendTaskRequest, session: UserSession
) -> None:
    task_token = request.message.token
    task_info = parse_task_token(
        task_token, gateway_config.secret_key, session.user.username
    )
    service_backend = await session.get_service_backend()
    task = await service_backend.get_task(task_info.task_uid)
    if task is None:
        raise InvalidTaskError("Task no longer exists")

    await service_backend.produce_unrouted_task(task)

    await send_success(websocket)


def is_valid_task_status_transition(old_status: TaskState, new_status: TaskState):
    if old_status is TaskState.SPAWNED and new_status in (
        TaskState.STARTED,
        TaskState.FINISHED,
        TaskState.CRASHED,
    ):
        return True
    if old_status is TaskState.STARTED and new_status in (
        TaskState.FINISHED,
        TaskState.CRASHED,
    ):
        return True
    return False


async def handle_set_task_status_request(
    websocket: WebSocket, request: SetTaskStatusRequest, session: UserSession
) -> None:
    task_token = request.message.token
    task_info = parse_task_token(
        task_token, gateway_config.secret_key, session.user.username
    )
    service_backend = await session.get_service_backend()
    task = await service_backend.get_task(task_info.task_uid)
    if task is None:
        raise InvalidTaskError("Task no longer exists")

    new_task_status = request.message.status
    if not is_valid_task_status_transition(
        old_status=task.status, new_status=new_task_status
    ):
        raise InvalidTaskError(
            f"Can't change task status from {task.status.value}"
            f" to {new_task_status.value}"
        )

    if new_task_status is TaskState.CRASHED:
        task.error = request.message.error

    await service_backend.set_task_status(task, new_task_status)
    await send_success(websocket)


async def generate_resource_download_urls(
    task: Task, service_backend: KartonAsyncBackend
) -> list[ResourceDownloadUrl]:
    resources = {}

    for resource in task.iterate_resources():
        if resource.uid in resources:
            continue
        if resource.bucket != service_backend.default_bucket_name:
            raise InvalidTaskError(
                "Got task that references different bucket than the configured one "
                "which is unsupported"
            )
        download_url = await service_backend.get_presigned_object_upload_url(
            bucket=service_backend.default_bucket_name, object_uid=resource.uid
        )
        resources[resource.uid] = ResourceDownloadUrl(
            uid=resource.uid, download_url=download_url
        )
    return list(resources.values())


async def handle_get_task_request(
    websocket: WebSocket, request: GetTaskRequest, session: UserSession
) -> None:
    if session.get_service_type() is not KartonServiceType.consumer:
        raise InvalidBindError("Only consumers can get tasks")

    identity = session.get_identity()
    service_backend = await session.get_service_backend()
    task = await service_backend.consume_routed_task(identity)
    if not task:
        raise NoTaskError("No task found, try again")

    try:
        resources = await generate_resource_download_urls(task, service_backend)
        task_token_info = TaskTokenInfo(
            task_uid=task.uid,
            resources=[r.uid for r in resources],
        )
        task_token = make_task_token(
            task_token_info, gateway_config.secret_key, session.user.username
        )
        incoming_task = IncomingTask(
            uid=task.uid,
            parent_uid=task.parent_uid,
            orig_uid=task.orig_uid,
            headers=task.headers,
            payload=task.payload,
            headers_persistent=task.headers_persistent,
            payload_persistent=task.payload_persistent,
            priority=task.priority,
        )
        task_response_message = TaskResponseMessage(
            task=incoming_task,
            token=task_token,
            resources=resources,
        )
        task_response = TaskResponse(message=task_response_message)
        await websocket.send_json(task_response.model_dump(mode="json"))
    except Exception:
        # We need to crash gathered task if something went wrong in the process
        exc_info = sys.exc_info()
        task.error = traceback.format_exception(*exc_info)
        await service_backend.set_task_status(task, TaskState.CRASHED)
        raise


TRequest = TypeVar("TRequest", bound=Request)
RequestHandler = Callable[[WebSocket, TRequest, UserSession], Awaitable[None]]

REQUEST_HANDLERS: dict[Type, RequestHandler] = {
    BindRequest: handle_bind_request,
    DeclareTaskRequest: handle_declare_task_request,
    SendTaskRequest: handle_send_task_request,
    SetTaskStatusRequest: handle_set_task_status_request,
    GetTaskRequest: handle_get_task_request,
}


async def call_request_handler(
    websocket: WebSocket, request: Request, session: UserSession
) -> None:
    await REQUEST_HANDLERS[type(request.root)](websocket, request.root, session)
