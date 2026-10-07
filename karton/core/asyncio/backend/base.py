from typing import IO, Any, AsyncIterator, Protocol

from karton.core.asyncio.resource import LocalResource, RemoteResource
from karton.core.backend import KartonBind, KartonMetrics
from karton.core.task import Task, TaskState


class KartonAsyncBackendProtocol(Protocol):
    """
    Protocol that defines methods that KartonAsyncBackend must implement.

    Used by producers and consumers to avoid depending on a concrete implementation.

    This Protocol documents the user-facing backend interface. Concrete
    implementations (e.g. :class:`karton.core.asyncio.backend.KartonAsyncBackend`,
    :class:`karton.core.asyncio.backend.KartonAsyncGatewayBackend`) provide
    additional helper methods for internal use.
    """

    async def connect(self) -> None:
        """
        Initialize backend connections (e.g. Redis, S3).

        Must be called before any other method is used. Synchronous backends
        perform this step in their constructor.
        """

    async def declare_task(self, task: Task) -> None:
        """
        Declares a new task to send it to the queue.

        :param task: Task to declare
        """

    async def set_task_status(self, task: Task, status: TaskState) -> None:
        """
        Request task status change to be applied.

        :param task: Task object
        :param status: New task status (TaskState)
        """

    async def register_bind(self, bind: KartonBind) -> KartonBind | None:
        """
        Register bind for Karton consumer and return the old one.

        :param bind: KartonBind object with bind definition
        :return: Old KartonBind that was registered under this identity
        """

    async def produce_unrouted_task(self, task: Task) -> None:
        """
        Add given task to unrouted task (``karton.tasks``) queue.

        Task must be declared beforehand.

        :param task: Task object
        """

    async def consume_routed_task(self, identity: str, timeout: int = 5) -> Task | None:
        """
        Get routed task for given consumer identity.

        If there are no tasks, blocks until new one appears or timeout is reached.

        Raises :class:`karton.core.exceptions.BindExpiredError` if the bind
        has been overridden by a newer service version.

        :param identity: Karton service identity
        :param timeout: Waiting for task timeout in seconds (default: 5)
        :return: Task object or None if timeout has been reached
        """

    async def increment_metrics(self, metric: KartonMetrics, identity: str) -> None:
        """
        Increments metrics for given operation type and identity.

        :param metric: Operation metric type
        :param identity: Related Karton service identity
        """

    async def upload_resource(
        self,
        resource: LocalResource,
        content: bytes | IO[bytes],
    ) -> None:
        """
        Upload resource object to underlying object storage (S3).

        :param resource: Resource to upload
        :param content: Object content as bytes or file-like stream
        """

    async def upload_resource_from_file(
        self, resource: LocalResource, path: str
    ) -> None:
        """
        Upload resource object file to underlying object storage.

        :param resource: Resource to upload
        :param path: Path to the object content
        """

    async def download_resource(self, resource: RemoteResource) -> bytes:
        """
        Download resource object from object storage.

        :param resource: Resource to download
        :return: Content bytes
        """

    async def download_resource_to_file(
        self, resource: RemoteResource, path: str
    ) -> None:
        """
        Download resource object from object storage to file.

        :param resource: Resource to download
        :param path: Target file path
        """

    async def produce_log(
        self, log_record: dict[str, Any], logger_name: str, level: str
    ) -> bool:
        """
        Push new log record to the logs channel.

        :param log_record: Dictionary representation of a
            :class:`logging.LogRecord`, carrying its standard attributes (see
            `LogRecord attributes
            <https://docs.python.org/3/library/logging.html#logrecord-attributes>`_)
            along with the following Karton-specific fields:

            - ``type`` - always ``"log"``
            - ``message`` - the formatted log message
            - ``hostname`` - hostname of the producing machine
            - ``task_id`` - UID of the currently processed task, or
              ``"(no task)"`` if none
            - ``task`` - serialized current task, present only when a task is
              in context
            - ``excText``, ``excValue``, ``excTraceback``, ``excType`` -
              exception details, present only when the record carries exception
              information
        :param logger_name: Name of the logger to publish under, typically a
            Karton service identity (e.g. ``"karton.classifier"``).
        :param level: Uppercase log level name (e.g. ``"DEBUG"``, ``"INFO"``).
        :return: True if any active log consumer received log record
        """

    def consume_log(
        self,
        timeout: int = 5,
        logger_filter: str | None = None,
        level: str | None = None,
    ) -> AsyncIterator[dict[str, Any] | None]:
        """
        Subscribe to logs channel and yield subsequent log records
        or None if timeout has been reached.

        If you want to subscribe only to a specific logger name
        and/or log level, pass them via logger_filter and level arguments.

        :param timeout: Waiting for log record timeout in seconds (default: 5)
        :param logger_filter: Filter logs by logger name. ``None`` (default)
            matches all loggers. Supports Redis Pub/Sub glob patterns, e.g.
            ``"karton.*"`` matches all ``karton.*`` services. Otherwise an exact
            logger name (e.g. ``"karton.classifier"``).
        :param level: Uppercase log level name (e.g. ``"DEBUG"``, ``"INFO"``).
            ``None`` (default) matches all levels. Case-insensitive.
        :return: Dict with log record (see :meth:`produce_log` for the field
            format)

        .. note::
            The ``level`` filter is an exact match, not a threshold. Unlike
            Python's :meth:`logging.Logger.setLevel`, ``level="INFO"`` matches
            only logs recorded at the ``INFO`` level, not ``WARNING`` or higher.
        """
