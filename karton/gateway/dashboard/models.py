import json
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from karton.core.backend import KartonBind
from karton.core.task import Task, TaskPriority, TaskState


class SuccessResponseModel(BaseModel):
    status: Literal["success"] = "success"


class TaskResponseModel(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    task: Task = Field(exclude=True)
    uid: str
    orig_uid: str
    parent_uid: str | None
    root_uid: str
    headers: dict[str, Any]
    payload: dict[str, Any]
    headers_persistent: dict[str, Any]
    payload_persistent: dict[str, Any]
    priority: TaskPriority
    status: TaskState
    last_update: float
    error: list[str] | None

    def to_json(self, indent: int | None = None) -> str:
        return self.task.serialize(indent=indent)


class AnalysisResponseModel(BaseModel):
    uid: str
    queues: dict[str, list[TaskResponseModel]]


class QueueListEntryResponseModel(BaseModel):
    bind: KartonBind
    pending_tasks: int = 0
    crashed_tasks: int = 0
    active_consumers: int = 0


class QueueListResponseModel(BaseModel):
    queues: dict[str, QueueListEntryResponseModel]


class QueueResponseModel(BaseModel):
    bind: KartonBind
    pending_tasks: list[TaskResponseModel]
    crashed_tasks: list[TaskResponseModel]
    active_consumers: int = 0


class ServicesListEntryResponseModel(BaseModel):
    identity: str
    karton_version: str | None
    service_version: str | None
    active_instances: int = 0


class ServicesListResponseModel(BaseModel):
    services: list[ServicesListEntryResponseModel]
