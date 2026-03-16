from typing import IO, Any, AsyncIterator, Protocol

from karton.core.asyncio.resource import LocalResource, RemoteResource
from karton.core.backend import KartonBind, KartonMetrics
from karton.core.task import Task, TaskState


class KartonAsyncBackendProtocol(Protocol):
    """
    Protocol that defines methods that KartonAsyncBackend must implement.

    Used by producers and consumers to avoid depending on a concrete implementation.
    """

    async def connect(self) -> None: ...

    async def declare_task(self, task: Task) -> None: ...

    async def set_task_status(self, task: Task, status: TaskState) -> None: ...

    async def register_bind(self, bind: KartonBind) -> KartonBind | None: ...

    async def produce_unrouted_task(self, task: Task) -> None: ...

    async def consume_routed_task(
        self, identity: str, timeout: int = 5
    ) -> Task | None: ...

    async def increment_metrics(self, metric: KartonMetrics, identity: str) -> None: ...

    async def upload_resource(
        self,
        resource: LocalResource,
        content: bytes | IO[bytes],
    ) -> None: ...

    async def upload_resource_from_file(
        self, resource: LocalResource, path: str
    ) -> None: ...

    async def download_resource(self, resource: RemoteResource) -> bytes: ...

    async def download_resource_to_file(
        self, resource: RemoteResource, path: str
    ) -> None: ...

    async def produce_log(
        self, log_record: dict[str, Any], logger_name: str, level: str
    ) -> bool: ...

    def consume_log(
        self,
        timeout: int = 5,
        logger_filter: str | None = None,
        level: str | None = None,
    ) -> AsyncIterator[dict[str, Any] | None]: ...
