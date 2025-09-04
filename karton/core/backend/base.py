import dataclasses
import enum
from collections import namedtuple
from typing import IO, Any, Iterator, Protocol

from karton.core.resource import LocalResource, RemoteResource
from karton.core.task import Task, TaskState

KartonBind = namedtuple(
    "KartonBind",
    [
        "identity",
        "info",
        "version",
        "persistent",
        "filters",
        "service_version",
        "is_async",
    ],
)


class KartonMetrics(enum.Enum):
    TASK_PRODUCED = "karton.metrics.produced"
    TASK_CONSUMED = "karton.metrics.consumed"
    TASK_CRASHED = "karton.metrics.crashed"
    TASK_ASSIGNED = "karton.metrics.assigned"
    TASK_GARBAGE_COLLECTED = "karton.metrics.garbage-collected"


@dataclasses.dataclass(frozen=True)
class KartonServiceInfo:
    """
    Extended Karton service information.

    Instances of this dataclass are meant to be aggregated to count service replicas
    in Karton Dashboard. They're considered equal if identity and versions strings
    are the same.
    """

    identity: str
    karton_version: str
    service_version: str | None = None


class KartonBackendProtocol(Protocol):
    """
    Protocol that defines methods that KartonBackend must implement.

    Used by producers and consumers to avoid depending on a concrete implementation.
    """

    def declare_task(self, task: Task) -> None: ...

    def set_task_status(self, task: Task, status: TaskState) -> None: ...

    def register_bind(self, bind: KartonBind) -> KartonBind | None: ...

    def produce_unrouted_task(self, task: Task) -> None: ...

    def consume_routed_task(self, identity: str, timeout: int = 5) -> Task | None: ...

    def increment_metrics(self, metric: KartonMetrics, identity: str) -> None: ...

    def upload_object(
        self,
        resource: LocalResource,
        content: bytes | IO[bytes],
    ) -> None: ...

    def upload_object_from_file(self, resource: LocalResource, path: str) -> None: ...

    def download_object(self, resource: RemoteResource) -> bytes: ...

    def download_object_to_file(self, resource: RemoteResource, path: str) -> None: ...

    def produce_log(
        self, log_record: dict[str, Any], logger_name: str, level: str
    ) -> bool: ...

    def consume_log(
        self,
        timeout: int = 5,
        logger_filter: str | None = None,
        level: str | None = None,
    ) -> Iterator[dict[str, Any] | None]: ...
