from typing import Any, Literal

from pydantic import BaseModel, Field, RootModel

from karton.core.backend import KartonBind
from karton.core.task import TaskPriority, TaskState

# Dict[Never, Never] would be better but is not supported by Pydantic
EmptyDict = dict


class ErrorResponseMessage(BaseModel):
    code: str
    error_message: str


class ErrorResponse(BaseModel):
    response: Literal["error"] = "error"
    message: ErrorResponseMessage


class SuccessResponse(BaseModel):
    response: Literal["success"] = "success"
    message: EmptyDict = Field(default_factory=dict)


class HelloResponseMessage(BaseModel):
    server_version: str
    auth_required: bool


class HelloResponse(BaseModel):
    response: Literal["hello"] = "hello"
    message: HelloResponseMessage


class AuthCredentials(BaseModel):
    username: str
    password: str


class HelloRequestMessage(BaseModel):
    identity: str
    service_version: str | None
    library_version: str
    secondary_connection: bool
    credentials: AuthCredentials | None = None


class HelloRequest(BaseModel):
    request: Literal["hello"] = "hello"
    message: HelloRequestMessage


class BindRequestMessage(BaseModel):
    info: str
    filters: list[dict[str, Any]]
    persistent: bool
    is_async: bool


class BindRequest(BaseModel):
    request: Literal["bind"] = "bind"
    message: BindRequestMessage


class BindResponseMessage(BaseModel):
    old_bind: KartonBind | None


class BindResponse(BaseModel):
    response: Literal["bind"] = "bind"
    message: BindResponseMessage


class NewTaskParameters(BaseModel):
    headers: dict[str, Any]
    payload: dict[str, Any]
    headers_persistent: dict[str, Any]
    payload_persistent: dict[str, Any]
    priority: TaskPriority


class DeclaredResourceSpec(BaseModel):
    """
    Gateway-specific resource schema for __karton_resource__ keys passed via
    NewTaskParameters.payload and NewTaskParameters.payload_persistent
    """

    uid: str
    name: str
    size: int
    metadata: dict[str, Any]
    sha256: str
    to_upload: bool = False


class DeclareTaskRequestMessage(BaseModel):
    task: NewTaskParameters
    parent_token: str | None = None


class DeclareTaskRequest(BaseModel):
    request: Literal["declare_task"] = "declare_task"
    message: DeclareTaskRequestMessage


class ResourceUrl(BaseModel):
    uid: str
    url: str


class TaskDeclaredResponseMessage(BaseModel):
    uid: str
    token: str
    upload_urls: list[ResourceUrl]


class TaskDeclaredResponse(BaseModel):
    response: Literal["task_declared"] = "task_declared"
    message: TaskDeclaredResponseMessage


class SendTaskRequestMessage(BaseModel):
    token: str


class SendTaskRequest(BaseModel):
    request: Literal["send_task"] = "send_task"
    message: SendTaskRequestMessage


class SetTaskStatusRequestMessage(BaseModel):
    token: str
    status: TaskState
    error: list[str] | None = None


class SetTaskStatusRequest(BaseModel):
    request: Literal["set_task_status"] = "set_task_status"
    message: SetTaskStatusRequestMessage


class GetTaskRequest(BaseModel):
    request: Literal["get_task"] = "get_task"


class IncomingTask(BaseModel):
    uid: str
    parent_uid: str | None
    orig_uid: str | None
    headers: dict[str, Any]
    payload: dict[str, Any]
    headers_persistent: dict[str, Any]
    payload_persistent: dict[str, Any]
    priority: TaskPriority


class TaskResponseMessage(BaseModel):
    task: IncomingTask
    token: str
    download_urls: list[ResourceUrl]


class TaskResponse(BaseModel):
    response: Literal["task"] = "task"
    message: TaskResponseMessage


class SendLogRequestMessage(BaseModel):
    log_record: dict[str, Any]
    logger_name: str
    level: str


class SendLogRequest(BaseModel):
    request: Literal["send_log"] = "send_log"
    message: SendLogRequestMessage


class LogSentResponseMessage(BaseModel):
    was_received: bool


class LogSentResponse(BaseModel):
    response: Literal["log_sent"] = "log_sent"
    message: LogSentResponseMessage


class SubscribeLogsRequestMessage(BaseModel):
    logger_filter: str | None
    level: str | None


class SubscribeLogsRequest(BaseModel):
    request: Literal["subscribe_logs"] = "subscribe_logs"
    message: SubscribeLogsRequestMessage


class LogResponseMessage(BaseModel):
    log_record: dict[str, Any]


class LogResponse(BaseModel):
    response: Literal["log"] = "log"
    message: LogResponseMessage


RequestType = (
    BindRequest
    | DeclareTaskRequest
    | SendTaskRequest
    | SetTaskStatusRequest
    | GetTaskRequest
    | SendLogRequest
    | SubscribeLogsRequest
)


class Request(RootModel):
    root: RequestType = Field(discriminator="request")
