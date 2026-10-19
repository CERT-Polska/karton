import dataclasses
import enum
import uuid
from collections.abc import Iterator
from typing import IO, Any, Literal, Protocol, Self
from urllib.parse import parse_qsl, urlencode

from karton.core.__version__ import __version__
from karton.core.exceptions import InvalidIdentityError
from karton.core.resource import LocalResource, RemoteResource
from karton.core.task import Task, TaskState


@dataclasses.dataclass(frozen=True, order=True)
class KartonBind:
    identity: str
    info: str | None
    version: str
    persistent: bool
    filters: list[dict[str, Any]]
    service_version: str | None
    is_async: bool


class KartonMetrics(enum.Enum):
    TASK_PRODUCED = "karton.metrics.produced"
    TASK_CONSUMED = "karton.metrics.consumed"
    TASK_CRASHED = "karton.metrics.crashed"
    TASK_ASSIGNED = "karton.metrics.assigned"
    TASK_GARBAGE_COLLECTED = "karton.metrics.garbage-collected"


@dataclasses.dataclass(frozen=True, order=True)
class KartonExternalServiceInfo:
    """
    Karton service information parsed from Redis/Gateway.

    Instances of this dataclass are meant to be aggregated to count service replicas
    in Karton Dashboard. They're considered equal if identity and versions strings
    are the same.
    """

    identity: str = dataclasses.field(metadata={"serializable": False})
    # This is optional only for compatibility reasons to represent
    # the services that have identity but don't provide extended version
    # information (with_service_info)
    karton_version: str | None = None
    service_version: str | None = None
    # instance_id is required for services connecting to Karton Gateway
    instance_id: uuid.UUID | None = None
    debug: bool = False
    # Extra information about Redis client
    redis_client_info: dict[str, str] | None = dataclasses.field(
        default=None, hash=False, compare=False, metadata={"serializable": False}
    )

    @classmethod
    def parse_client_name(
        cls, client_name: str, redis_client_info: dict[str, str] | None = None
    ) -> "KartonExternalServiceInfo":
        included_keys = [
            field.name
            for field in dataclasses.fields(cls)
            if field.metadata.get("serializable", True)
        ]
        params: dict[str, Any]
        if "?" in client_name:
            identity, params_string = client_name.split("?", 1)
            # Filter out unknown params to not get crashed by future extensions
            params = {
                key: value
                for key, value in parse_qsl(params_string)
                if key in included_keys
            }
        else:
            identity = client_name
            params = {}

        if "debug" in params and params["debug"] == "1":
            params["debug"] = True

        if "instance_id" in params:
            try:
                params["instance_id"] = uuid.UUID(params["instance_id"])
            except ValueError:
                del params["instance_id"]

        return KartonExternalServiceInfo(
            identity, redis_client_info=redis_client_info, **params
        )


@dataclasses.dataclass(frozen=True, order=True)
class KartonServiceInfo:
    """
    Extended Karton service information.
    """

    identity: str
    instance_id: uuid.UUID
    karton_version: str = __version__
    service_version: str | None = None
    debug: bool = False

    @classmethod
    def create(cls, identity: str, service_version: str | None = None) -> Self:
        """
        Creates a KartonServiceInfo object that represent single instance
        of Karton service.

        The main use case of this method is to easily instantiate the
        service_info parameters with random instance_id when KartonBackend
        is used directly.
        """
        return cls(
            identity=identity,
            instance_id=uuid.uuid4(),
            service_version=service_version,
        )

    def __post_init__(self) -> None:
        disallowed_chars = [" ", "?"]
        if not self.identity:
            raise InvalidIdentityError(
                "Karton identity should be set to non-empty string"
            )
        if any(
            disallowed_char in self.identity for disallowed_char in disallowed_chars
        ):
            raise InvalidIdentityError(
                f"Karton identity must not contain {disallowed_chars}"
            )

    def make_client_name(self) -> str:
        params: dict[str, str] = {
            "instance_id": str(self.instance_id),
            "karton_version": self.karton_version,
        }
        if self.service_version is not None:
            params["service_version"] = self.service_version
        if self.debug:
            params["debug"] = "1"
        if params:
            return f"{self.identity}?{urlencode(params)}"
        else:
            return self.identity


def make_debug_identity(identity: str, instance_id: uuid.UUID) -> str:
    """
    Make debug identity bound to the instance id to create ephemeral task queue
    for debugging purposes
    """
    return f"{identity}-{instance_id!s}-dev"


def unserialize_bind(identity: str, bind: dict[str, Any]) -> KartonBind:
    """
    Creates a KartonBind from identity and bind data
    """
    return KartonBind(
        identity=identity,
        info=bind["info"],
        version=bind["version"],
        persistent=bind["persistent"],
        filters=bind["filters"],
        service_version=bind.get("service_version"),
        is_async=bind.get("is_async", False),
    )


@dataclasses.dataclass(frozen=True)
class KartonOutputs:
    """
    Represents the output types declared by a Karton service.
    """

    identity: str
    outputs: list[dict[str, Any]]


@dataclasses.dataclass
class KartonQueueState:
    """
    Karton queue state, returned by :meth:`KartonBackendProtocol.get_queues`.
    """

    bind: KartonBind
    replicas: int
    pending_task_uids: list[str] = dataclasses.field(default_factory=list)
    crashed_task_uids: list[str] = dataclasses.field(default_factory=list)


class KartonBackendProtocol(Protocol):
    """
    Protocol that defines methods that KartonBackend must implement.

    Used by producers and consumers to avoid depending on a concrete implementation.

    This Protocol documents the user-facing backend interface. Concrete
    implementations (e.g. :class:`karton.core.backend.KartonBackend`,
    :class:`karton.core.backend.KartonGatewayBackend`) provide additional
    helper methods for internal use.
    """

    def declare_task(self, task: Task) -> None:
        """
        Declares a new task to send it to the queue.

        :param task: Task to declare
        """

    def set_task_status(self, task: Task, status: TaskState) -> None:
        """
        Request task status change to be applied.

        :param task: Task object
        :param status: New task status (TaskState)
        """

    def register_bind(self, bind: KartonBind) -> KartonBind | None:
        """
        Register bind for Karton consumer and return the old one.

        :param bind: KartonBind object with bind definition
        :return: Old KartonBind that was registered under this identity
        """

    def produce_unrouted_task(self, task: Task) -> None:
        """
        Add given task to unrouted task (``karton.tasks``) queue.

        Task must be declared beforehand.

        :param task: Task object
        """

    def consume_routed_task(self, identity: str, timeout: int = 5) -> Task | None:
        """
        Get routed task for given consumer identity.

        If there are no tasks, blocks until new one appears or timeout is reached.

        Raises :class:`karton.core.exceptions.BindExpiredError` if the bind
        has been overridden by a newer service version.

        :param identity: Karton service identity
        :param timeout: Waiting for task timeout in seconds (default: 5)
        :return: Task object or None if timeout has been reached
        """

    def increment_metrics(self, metric: KartonMetrics, identity: str) -> None:
        """
        Increments metrics for given operation type and identity.

        :param metric: Operation metric type
        :param identity: Related Karton service identity
        """

    def upload_resource(
        self,
        resource: LocalResource,
        content: bytes | IO[bytes],
    ) -> None:
        """
        Upload resource object to underlying object storage (S3).

        :param resource: Resource to upload
        :param content: Object content as bytes or file-like stream
        """

    def upload_resource_from_file(self, resource: LocalResource, path: str) -> None:
        """
        Upload resource object file to underlying object storage.

        :param resource: Resource to upload
        :param path: Path to the object content
        """

    def iter_download_resource(
        self, resource: RemoteResource, chunk_size: int = 64 * 1024
    ) -> Iterator[bytes]:
        """
        Download resource object, yielding its content as chunks of bytes.

        :param resource: Resource to download
        :param chunk_size: Chunk size in bytes
        :return: Iterator of content chunks
        """

    def download_resource(self, resource: RemoteResource) -> bytes:
        """
        Download resource object from object storage.

        :param resource: Resource to download
        :return: Content bytes
        """

    def download_resource_to_file(self, resource: RemoteResource, path: str) -> None:
        """
        Download resource object from object storage to file.

        :param resource: Resource to download
        :param path: Target file path
        """

    def produce_log(
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
    ) -> Iterator[dict[str, Any] | None]:
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

    def get_binds(self) -> list[KartonBind]:
        """
        Get all binds registered in the backend.

        :return: List of KartonBind objects for all registered identities
        """

    def get_bind(self, identity: str) -> KartonBind | None:
        """
        Get bind object for given identity.

        :param identity: Karton service identity
        :return: KartonBind object or None if not found
        """

    def get_online_services(self) -> list[KartonExternalServiceInfo]:
        """
        Get all online services providing extended service information.

        .. versionadded:: 5.1.0

        :return: List of KartonExternalServiceInfo objects
        """

    def get_online_identities(self) -> dict[str, list[KartonExternalServiceInfo]]:
        """
        Returns a dictionary for all online identities with list of
        KartonExternalServiceInfo of each instance.

        :return: Dictionary {identity: [list of service info objects]}
        """

    def get_queues(self, parse_resources: bool = False) -> dict[str, KartonQueueState]:
        """
        Get a summary of all queues (binds) and their task state.

        For the direct backend, full Task objects are included in each
        :class:`QueueSummary`. For the gateway backend, only task UIDs are
        included and full tasks are fetched lazily.

        :return: Dictionary mapping queue identity to :class:`QueueSummary`
        """

    def get_tasks(self, task_uids: list[str]) -> list[Task]:
        """
        Get multiple task objects by their identifiers.

        Tasks that don't exist are silently skipped.

        :param task_uids: List of task identifiers
        :return: List of Task objects (may be shorter than the input if some
            tasks were not found)
        """

    def get_analysis_tasks(self, root_uid: str) -> dict[str, list[Task]]:
        """
        Get all tasks belonging to the same analysis tree, grouped by
        receiver queue.

        Includes both pending and crashed tasks (excludes only FINISHED).

        :param root_uid: Root identifier of the analysis task tree
        :return: Dictionary mapping receiver queue identity to list of tasks
        """

    def get_task(self, task_uid: str) -> Task | None:
        """
        Get task object with given identifier.

        :param task_uid: Task identifier
        :return: Task object or None if not found
        """

    def restart_task(self, task: Task) -> Task:
        """
        Requeue a consumed task back to the consumer queue.

        A new task is created with a new UID. The original task is marked
        as finished.

        :param task: Task to be restarted
        :return: The restarted (new) task object
        """

    def cancel_task(self, task: Task) -> None:
        """
        Cancel a task by marking it as finished.

        :param task: Task to be cancelled
        """

    def restart_queue_tasks(
        self,
        identity: str,
        status: Literal["crashed", "pending"] = "crashed",
    ) -> list[tuple[str, str]]:
        """
        Restart all tasks matching the given status in a queue.

        :param identity: Karton service identity (queue name)
        :param status: Which tasks to restart (``"crashed"`` or ``"pending"``)
        :return: List of ``(old_uid, new_uid)`` pairs for each restarted task
        """

    def cancel_queue_tasks(
        self,
        identity: str,
        status: Literal["crashed", "pending"] = "crashed",
    ) -> None:
        """
        Cancel all tasks matching the given status in a queue.

        :param identity: Karton service identity (queue name)
        :param status: Which tasks to cancel (``"crashed"`` or ``"pending"``)
        """

    def get_outputs(self) -> list[KartonOutputs]:
        """
        Get a list of the output types for each Karton service.

        :return: List of KartonOutputs objects
        """
