from typing import Any, Literal

from pydantic import BaseModel

from karton.core.backend import KartonBind
from karton.core.task import TaskPriority, TaskState


class SuccessResponseModel(BaseModel):
    status: Literal["success"] = "success"


class TaskResponseModel(BaseModel):
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
