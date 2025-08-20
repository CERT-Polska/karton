from typing import Any, Literal

from pydantic import BaseModel, Field, RootModel

from karton.core.task import TaskPriority, TaskState


class HelloResponseMessage(BaseModel):
    server_version: str
    auth_required: bool


class HelloResponse(BaseModel):
    response: Literal["hello"] = "hello"
    message: HelloResponseMessage


class ErrorResponseMessage(BaseModel):
    code: str
    error_message: str


class ErrorResponse(BaseModel):
    response: Literal["error"] = "error"
    message: ErrorResponseMessage


class SuccessResponse(BaseModel):
    response: Literal["success"] = "success"


class AuthRequestMessage(BaseModel):
    username: str
    password: str


class AuthRequest(BaseModel):
    request: Literal["auth"] = "auth"
    message: AuthRequestMessage


class ConsumerBindRequestMessage(BaseModel):
    service_type: Literal["consumer"] = "consumer"
    identity: str
    info: str | None
    library_version: str
    service_version: str | None
    filters: list[dict[str, Any]]
    persistent: bool
    is_async: bool


class ProducerBindRequestMessage(BaseModel):
    service_type: Literal["producer"] = "producer"
    identity: str
    library_version: str
    service_version: str | None


class LogConsumerBindRequestMessage(BaseModel):
    service_type: Literal["log_consumer"] = "log_consumer"
    identity: str
    library_version: str
    service_version: str | None


class BindRequest(BaseModel):
    request: Literal["bind"] = "bind"
    message: (
        ConsumerBindRequestMessage
        | ProducerBindRequestMessage
        | LogConsumerBindRequestMessage
    ) = Field(discriminator="service_type")


class NewTaskParameters(BaseModel):
    headers: dict[str, Any]
    payload: dict[str, Any]
    headers_persistent: dict[str, Any]
    payload_persistent: dict[str, Any]
    priority: TaskPriority


class DeclaredResourceSpec(BaseModel):
    """
    Gateway-specific resource schema for __karton_resource__ keys. Partially
    compatible with karton.core.resource schema but doesn't support "bucket"
    key (we don't support references to other buckets) and contains "to_upload"
    mark to distinguish LocalResource from RemoteResource.
    """

    uid: str
    name: str
    size: int
    metadata: dict[str, Any]
    sha256: str
    to_upload: bool = False


class DeclareTaskRequestMessage(BaseModel):
    task: NewTaskParameters
    token: str | None = None


class DeclareTaskRequest(BaseModel):
    request: Literal["declare_task"] = "declare_task"
    message: DeclareTaskRequestMessage


class ResourceUploadUrl(BaseModel):
    uid: str
    upload_url: str


class TaskDeclaredResponseMessage(BaseModel):
    uid: str
    resources_to_upload: list[ResourceUploadUrl]
    token: str


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


class ResourceDownloadUrl(BaseModel):
    uid: str
    download_url: str


class TaskResponseMessage(BaseModel):
    task: IncomingTask
    token: str
    resources: list[ResourceDownloadUrl]


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


class Request(RootModel):
    root: (
        BindRequest
        | DeclareTaskRequest
        | SendTaskRequest
        | SetTaskStatusRequest
        | GetTaskRequest
        | SendLogRequest
    ) = Field(discriminator="request")
