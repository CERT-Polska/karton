import dataclasses
import enum
import urllib.parse
from collections import namedtuple
from typing import Optional, Protocol, Union, IO, Dict, Any, Iterator

from karton.core import Task, LocalResource, RemoteResource
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

@dataclasses.dataclass(frozen=True, order=True)
class KartonServiceInfo:
    """
    Karton service information
    """

    identity: str
    karton_version: str
    service_version: Optional[str] = None

class KartonBackendProtocol(Protocol):
    """
    Interface for KartonBackend high-level methods used by producers and consumers
    """

    def declare_task(self, task: Task) -> None: ...

    def set_task_status(self, task: Task, status: TaskState) -> None: ...

    def register_bind(self, bind: KartonBind) -> None: ...

    def produce_unrouted_task(self, task: Task) -> None: ...

    def consume_routed_task(
        self, identity: str, timeout: int = 5, _bind: Optional[KartonBind] = None
    ) -> Optional[Task]: ...

    def increment_metrics(self, metric: KartonMetrics, identity: str) -> None: ...

    def upload_object(
        self, resource: LocalResource, content: Union[bytes, IO[bytes]]
    ) -> None: ...

    def upload_object_from_file(self, resource: LocalResource, path: str) -> None: ...

    def download_object(self, resource: RemoteResource) -> bytes: ...

    def download_object_to_file(self, resource: RemoteResource, path: str) -> None: ...

    def produce_log(
        self, log_record: Dict[str, Any], logger_name: str, level: str
    ) -> bool: ...

    def remove_object(self, bucket: str, object_uid: str) -> None: ...

    def consume_log(
        self,
        timeout: int = 5,
        logger_filter: Optional[str] = None,
        level: Optional[str] = None,
    ) -> Iterator[Optional[Dict[str, Any]]]: ...

def make_redis_client_name(service_info: KartonServiceInfo) -> str:
    params = {
        "karton_version": service_info.karton_version,
        "service_type": service_info.service_type.value,
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