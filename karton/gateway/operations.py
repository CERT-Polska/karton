import sys
import traceback
from typing import TYPE_CHECKING, Protocol, Type, TypeVar, cast

from fastapi import WebSocket

from karton.core.asyncio.backend import KartonBind, KartonMetrics
from karton.core.exceptions import BindExpiredError as KartonBindExpiredError
from karton.core.task import Task, TaskState

from .backend import gateway_backend
from .config import gateway_config
from .errors import (
    AlreadyBoundError,
    GatewayBindExpiredError,
    InvalidBindError,
    InvalidTaskError,
    InvalidTaskStatusError,
    OperationTimeoutError,
    ShutdownError,
)
from .messages import send_success
from .shutdown import shutdown_latch
from .task import (
    TaskTokenInfo,
    generate_resource_download_urls,
    generate_resource_upload_urls,
    is_valid_task_status_transition,
    make_task_token,
    parse_task_token,
    process_declared_task_resources,
)

if TYPE_CHECKING:
    from .session import ClientSession

from .models import (
    BindRequest,
    BindResponse,
    BindResponseMessage,
    DeclareTaskRequest,
    GetTaskRequest,
    IncomingTask,
    LogResponse,
    LogResponseMessage,
    LogSentResponse,
    LogSentResponseMessage,
    Request,
    RequestType,
    SendLogRequest,
    SendTaskRequest,
    SetTaskStatusRequest,
    SubscribeLogsRequest,
    TaskDeclaredResponse,
    TaskDeclaredResponseMessage,
    TaskResponse,
    TaskResponseMessage,
)

T = TypeVar("T", bound=RequestType, contravariant=True)


class RequestHandler(Protocol[T]):
    async def __call__(
        self, websocket: WebSocket, request: T, session: "ClientSession"
    ) -> None: ...


REQUEST_HANDLERS: dict[Type[RequestType], RequestHandler] = {}


def request_handler(
    handler_fn: RequestHandler[T],
) -> RequestHandler[T]:
    request_type = handler_fn.__annotations__["request"]
    if request_type in REQUEST_HANDLERS:
        raise ValueError(f"Handler for request type {request_type} is already defined")
    REQUEST_HANDLERS[request_type] = handler_fn
    return handler_fn


async def call_request_handler(
    websocket: WebSocket, request: Request, session: "ClientSession"
) -> None:
    await REQUEST_HANDLERS[type(request.root)](websocket, request.root, session)


@request_handler
async def handle_bind_request(
    websocket: WebSocket, request: BindRequest, session: "ClientSession"
) -> None:
    """
    Handler for 'bind' request which implements register_bind operation
    in the backend. Creates a new consumer queue or connects to existing one.

    :param websocket: WebSocket that received the request
    :param request: Parsed request object
    :param session: Client session that initiated the request
    """
    if session.is_bound:
        raise AlreadyBoundError("Client is already bound")

    bind = KartonBind(
        identity=session.service_info.identity,
        info=request.message.info,
        version=cast(str, session.service_info.karton_version),
        persistent=request.message.persistent,
        filters=request.message.filters,
        service_version=session.service_info.service_version,
        is_async=request.message.is_async,
    )
    old_bind = await gateway_backend.register_bind(bind)
    session.karton_bind = bind
    bind_response_message = BindResponseMessage(
        old_bind=old_bind,
    )
    bind_response = BindResponse(message=bind_response_message)
    await websocket.send_text(bind_response.model_dump_json())


@request_handler
async def handle_declare_task_request(
    websocket: WebSocket, request: DeclareTaskRequest, session: "ClientSession"
) -> None:
    """
    Handler for 'declare_task' request which implements declare_task operation
    in the backend. Validates the task declaration and declares a new task in Karton
    to lock the resources before they're uploaded.

    :param websocket: WebSocket that received the request
    :param request: Parsed request object
    :param session: Client session that initiated the request
    """
    parent_token = request.message.parent_token
    task_params = request.message.task

    if parent_token is None:
        parent_task_uid = None
        allowed_parent_resources = []
    else:
        parent_task_info = parse_task_token(
            token=parent_token,
            secret_key=gateway_config.secret_key,
            audience=session.identity,
        )
        parent_task_uid = parent_task_info.task_uid
        allowed_parent_resources = parent_task_info.resources

    allowed_buckets = [
        gateway_backend.default_bucket_name
    ] + gateway_config.allowed_extra_buckets
    payload_bags = (task_params.payload, task_params.payload_persistent)

    # Now, we need to translate DeclaredResourceSpec to RemoteResource
    task_payload_bags, resources = process_declared_task_resources(
        payload_bags, allowed_parent_resources, allowed_buckets
    )
    resource_urls = await generate_resource_upload_urls(resources)
    task_payload, task_payload_persistent = task_payload_bags

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
        audience=session.identity,
    )

    await gateway_backend.register_task(task)
    task_declared_message = TaskDeclaredResponseMessage(
        uid=task.uid,
        token=task_token,
        upload_urls=resource_urls,
    )
    task_declared_request = TaskDeclaredResponse(message=task_declared_message)
    await websocket.send_text(task_declared_request.model_dump_json())


@request_handler
async def handle_send_task_request(
    websocket: WebSocket, request: SendTaskRequest, session: "ClientSession"
) -> None:
    """
    Handler for 'send_task' request which implements send_task operation
    in the backend. Accepts a task token returned by former response for
    declare_task request.

    :param websocket: WebSocket that received the request
    :param request: Parsed request object
    :param session: Client session that initiated the request
    """
    task_token = request.message.token
    task_info = parse_task_token(
        token=task_token,
        secret_key=gateway_config.secret_key,
        audience=session.identity,
    )
    task = await gateway_backend.get_task(task_info.task_uid)
    if task is None:
        raise InvalidTaskError("Task no longer exists")
    if task.status is not TaskState.DECLARED:
        raise InvalidTaskError(
            f"Task status is '{task.status.value}' while only "
            f"'declared' tasks can be sent"
        )

    await gateway_backend.produce_unrouted_task(task)
    await gateway_backend.increment_metrics(
        KartonMetrics.TASK_PRODUCED, session.identity
    )
    await send_success(websocket)


@request_handler
async def handle_set_task_status_request(
    websocket: WebSocket, request: SetTaskStatusRequest, session: "ClientSession"
) -> None:
    """
    Handler for 'set_task_status' request which implements set_task_status operation
    in the backend. Used for marking task as:

    - STARTED when consumer starts to execute the task
    - FINISHED when consumer finishes executing the task
    - CRASHED when task execution resulted in an error

    :param websocket: WebSocket that received the request
    :param request: Parsed request object
    :param session: Client session that initiated the request
    """
    task_token = request.message.token
    task_info = parse_task_token(
        token=task_token,
        secret_key=gateway_config.secret_key,
        audience=session.identity,
    )
    task = await gateway_backend.get_task(task_info.task_uid)
    if task is None:
        raise InvalidTaskError("Task no longer exists")

    new_task_status = request.message.status
    if not is_valid_task_status_transition(
        old_status=task.status, new_status=new_task_status
    ):
        raise InvalidTaskStatusError(
            f"Can't change task status from {task.status.value}"
            f" to {new_task_status.value}"
        )

    if new_task_status is TaskState.CRASHED:
        task.error = request.message.error
        await gateway_backend.increment_metrics(
            KartonMetrics.TASK_CRASHED, session.identity
        )

    await gateway_backend.set_task_status(task, new_task_status)
    await gateway_backend.increment_metrics(
        KartonMetrics.TASK_CONSUMED, session.identity
    )
    await send_success(websocket)


@request_handler
async def handle_get_task_request(
    websocket: WebSocket, request: GetTaskRequest, session: "ClientSession"
) -> None:
    """
    Handler for 'get_task' request which implements consume_routed_task operation
    in the backend.

    If there is a task in the queue: returns it along with resource
    download URLs. If there is no task and operation reaches timeout (5 seconds),
    returns 'timeout' error. In that case, client should retry the request and continue
    operation, unless requested by user to shut down.

    Before consuming a task, checks whether binds have not been replaced by a newer
    version of a consumer. In that case, returns 'expired_bind' error.

    :param websocket: WebSocket that received the request
    :param request: Parsed request object
    :param session: User session that initiated the request
    """
    if not session.is_bound:
        raise InvalidBindError("Service has not declared consumer bind")

    try:
        task = await gateway_backend.consume_routed_task(session.service_info.identity)
    except KartonBindExpiredError as e:
        raise GatewayBindExpiredError(str(e)) from e

    if not task:
        raise OperationTimeoutError("No task found, try again")

    try:
        allowed_buckets = [
            gateway_backend.default_bucket_name
        ] + gateway_config.allowed_extra_buckets
        download_urls = await generate_resource_download_urls(task, allowed_buckets)
        task_token_info = TaskTokenInfo(
            task_uid=task.uid,
            resources=[r.uid for r in download_urls],
        )
        task_token = make_task_token(
            task_token_info=task_token_info,
            secret_key=gateway_config.secret_key,
            audience=session.identity,
        )
        task_data = task.to_dict()
        incoming_task = IncomingTask(
            uid=task_data["uid"],
            parent_uid=task_data["parent_uid"],
            orig_uid=task_data["orig_uid"],
            headers=task_data["headers"],
            payload=task_data["payload"],
            headers_persistent=task_data["headers_persistent"],
            payload_persistent=task_data["payload_persistent"],
            priority=task_data["priority"],
        )
        task_response_message = TaskResponseMessage(
            task=incoming_task,
            token=task_token,
            download_urls=download_urls,
        )
        task_response = TaskResponse(message=task_response_message)
        await websocket.send_text(task_response.model_dump_json())
    except Exception:
        # We need to crash gathered task if something went wrong in the process
        exc_info = sys.exc_info()
        task.error = traceback.format_exception(*exc_info)
        await gateway_backend.set_task_status(task, TaskState.CRASHED)
        await gateway_backend.increment_metrics(
            KartonMetrics.TASK_CRASHED, session.identity
        )
        await gateway_backend.increment_metrics(
            KartonMetrics.TASK_CONSUMED, session.identity
        )
        raise


@request_handler
async def handle_send_log_request(
    websocket: WebSocket, request: SendLogRequest, session: "ClientSession"
) -> None:
    """
    Handler for 'send_log' request which implements produce_log operation
    in the backend. Publishes a log record to be consumed by currently
    connected log consumers. Returns a bool response indicating whether
    the log was successfully received by at least one log consumer.

    :param websocket: WebSocket that received the request
    :param request: Parsed request object
    :param session: Client session that initiated the request
    """
    was_received = await gateway_backend.produce_log(
        request.message.log_record,
        request.message.logger_name,
        request.message.level,
    )
    log_sent_message = LogSentResponseMessage(was_received=was_received)
    log_sent_response = LogSentResponse(message=log_sent_message)
    await websocket.send_text(log_sent_response.model_dump_json())


@request_handler
async def handle_subscribe_logs_request(
    websocket: WebSocket, request: SubscribeLogsRequest, session: "ClientSession"
) -> None:
    """
    Handler for 'subscribe_log' request which implements consume_log operation
    in the backend. Returns a stream of log records matching the provided filter.
    If there is no log for 5 seconds, returns 'timeout' error. In that case,
    client should retry the request and continue operation unless requested by user
    to shut down.

    :param websocket: WebSocket that received the request
    :param request: Parsed request object
    :param session: User session that initiated the request
    """
    async for log_record in gateway_backend.consume_log(
        logger_filter=request.message.logger_filter, level=request.message.level
    ):
        if shutdown_latch.shutdown_in_progress:
            raise ShutdownError("Operation terminated, shutdown is in progress")
        if not log_record:
            log_record = {}
        log_message = LogResponseMessage(log_record=log_record)
        log_response = LogResponse(message=log_message)
        await websocket.send_text(log_response.model_dump_json())
