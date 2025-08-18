from typing import Any, Literal

from pydantic import BaseModel, Field, RootModel


class HelloResponseMessage(BaseModel):
    server_version: str


class HelloResponse(BaseModel):
    response: Literal["hello"] = "hello"
    message: HelloResponseMessage


class ErrorResponseMessage(BaseModel):
    code: str
    error_message: str
    details: dict


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
    info: str
    library_version: str
    service_version: str
    filters: list[dict[str, Any]]
    persistent: bool
    is_async: bool


class ProducerBindRequestMessage(BaseModel):
    service_type: Literal["producer"] = "producer"
    identity: str
    library_version: str
    service_version: str


class LogConsumerBindRequestMessage(BaseModel):
    service_type: Literal["log_consumer"] = "log_consumer"
    identity: str
    library_version: str
    service_version: str


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
    priority: Literal["high"] | Literal["normal"] | Literal["low"]


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
    resources: list[ResourceUploadUrl]
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
    status: str
    error: dict[str, Any] | None = None


class SetTaskStatusRequest(BaseModel):
    request: Literal["set_task_status"] = "set_task_status"
    message: SetTaskStatusRequestMessage


class GetTaskRequest(BaseModel):
    request: Literal["get_task"] = "get_task"


class IncomingTask(BaseModel):
    uid: str
    parent_uid: str
    orig_uid: str
    headers: dict[str, Any]
    payload: dict[str, Any]
    headers_persistent: dict[str, Any]
    payload_persistent: dict[str, Any]
    priority: Literal["high"] | Literal["normal"] | Literal["low"]


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


class Request(RootModel):
    root: (
        BindRequest
        | DeclareTaskRequest
        | SendTaskRequest
        | SetTaskStatusRequest
        | GetTaskRequest
    ) = Field(discriminator="request")
