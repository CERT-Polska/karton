import dataclasses
import enum
from collections import namedtuple
from typing import IO, Any, Iterator, Protocol

from karton.core.__version__ import __version__
from karton.core.exceptions import InvalidIdentityError
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


def resolve_service_info(
    identity: str | None, service_info: KartonServiceInfo | None
) -> KartonServiceInfo:
    """
    Makes KartonServiceInfo object from backend parameters that identify the service.

    If only identity is provided: KartonServiceInfo containing only an identity and
    karton_version information is created.

    This function also validates if identity is correct and doesn't contain disallowed
    characters.
    """
    if service_info is None:
        if identity is None:
            raise InvalidIdentityError(
                "Either identity or service_info must be provided"
            )
        service_info = KartonServiceInfo(
            identity=identity,
            karton_version=__version__,
        )
    disallowed_chars = [" ", "?"]
    if any(
        disallowed_char in service_info.identity for disallowed_char in disallowed_chars
    ):
        raise InvalidIdentityError(
            f"Karton identity must not contain {disallowed_chars}"
        )
    return service_info


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
