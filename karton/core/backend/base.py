import dataclasses
import enum
import urllib.parse
from collections import namedtuple
from typing import IO, Any, Iterator, Protocol

from karton.core import Task
from karton.core.task import TaskState

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
    Interface for KartonBackend high-level methods used by producers and consumers
    """

    def declare_task(self, task: Task) -> None: ...

    def set_task_status(self, task: Task, status: TaskState) -> None: ...

    def get_bind(self, identity: str) -> KartonBind: ...

    def register_bind(self, bind: KartonBind) -> KartonBind | None: ...

    def produce_unrouted_task(self, task: Task) -> None: ...

    def consume_routed_task(self, identity: str, timeout: int = 5) -> Task | None: ...

    def increment_metrics(self, metric: KartonMetrics, identity: str) -> None: ...

    def upload_object(
        self,
        bucket: str,
        object_uid: str,
        content: bytes | IO[bytes],
    ) -> None: ...

    def upload_object_from_file(
        self, bucket: str, object_uid: str, path: str
    ) -> None: ...

    def download_object(self, bucket: str, object_uid: str) -> bytes: ...

    def download_object_to_file(
        self, bucket: str, object_uid: str, path: str
    ) -> None: ...

    def produce_log(
        self, log_record: dict[str, Any], logger_name: str, level: str
    ) -> bool: ...

    def remove_object(self, bucket: str, object_uid: str) -> None: ...

    def consume_log(
        self,
        timeout: int = 5,
        logger_filter: str | None = None,
        level: str | None = None,
    ) -> Iterator[dict[str, Any] | None]: ...


def make_redis_client_name(service_info: KartonServiceInfo) -> str:
    params = {
        "karton_version": service_info.karton_version,
    }
    if service_info.service_version is not None:
        params.update({"service_version": service_info.service_version})
    return f"{service_info.identity}?{urllib.parse.urlencode(params)}"


def parse_redis_client_name(client_name: str) -> KartonServiceInfo:
    identity, params_string = client_name.split("?", 1)
    # Filter out unknown params to not get crashed by future extensions
    params = dict(urllib.parse.parse_qsl(params_string))
    karton_version = params.get("karton_version", "")
    service_version = params.get("service_version")
    return KartonServiceInfo(
        identity=identity,
        karton_version=karton_version,
        service_version=service_version,
    )
