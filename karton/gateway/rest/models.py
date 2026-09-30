from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from karton.core.task import TaskPriority, TaskState
from karton.gateway.models import ResourceUrl


class Bind(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    identity: str
    info: str | None
    version: str
    persistent: bool
    filters: list[dict[str, Any]]
    service_version: str | None
    is_async: bool


class ServiceInfo(BaseModel):
    identity: str
    karton_version: str | None
    service_version: str | None
    instance_id: str | None
    active_replicas: int


class QueueView(BaseModel):
    identity: str
    info: str | None
    version: str
    persistent: bool
    filters: list[dict[str, Any]]
    service_version: str | None
    is_async: bool
    replicas: int
    pending_task_uids: list[str]
    crashed_task_uids: list[str]


class TaskView(BaseModel):
    uid: str
    root_uid: str
    parent_uid: str | None
    orig_uid: str | None
    status: TaskState
    priority: TaskPriority
    last_update: float
    headers: dict[str, Any]
    headers_persistent: dict[str, Any]
    payload: dict[str, Any]
    payload_persistent: dict[str, Any]
    error: list[str] | None
    download_urls: list[ResourceUrl] = Field(default_factory=list)


class AnalysisView(BaseModel):
    uid: str
    queues: dict[str, list[TaskView]]


class ProducerOutput(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    identity: str
    outputs: list[dict[str, Any]]


class RestartedTask(BaseModel):
    uid: str


class APIError(BaseModel):
    error: str
