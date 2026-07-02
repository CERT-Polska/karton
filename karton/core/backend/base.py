import dataclasses
import enum
import uuid
from urllib.parse import urlencode, parse_qsl
from typing import IO, Any, Iterator, Protocol, Self

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
    instance_id: str | None = None
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
        if "?" in client_name:
            identity, params_string = client_name.split("?", 1)
            # Filter out unknown params to not get crashed by future extensions
            params = dict(
                [
                    (key, value)
                    for key, value in parse_qsl(params_string)
                    if key in included_keys
                ]
            )
        else:
            identity = client_name
            params = {}
        return KartonExternalServiceInfo(
            identity, redis_client_info=redis_client_info, **params
        )


@dataclasses.dataclass(frozen=True, order=True)
class KartonServiceInfo:
    """
    Extended Karton service information.
    """

    identity: str
    instance_id: str
    karton_version: str = __version__
    service_version: str | None = None

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
            instance_id=str(uuid.uuid4()),
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
        included_keys = [
            field.name
            for field in dataclasses.fields(self)
            if field.metadata.get("serializable", True)
        ]
        params = {
            k: v
            for k, v in dataclasses.asdict(self).items()
            if k in included_keys and v is not None
        }
        if params:
            return f"{self.identity}?{urlencode(params)}"
        else:
            return self.identity


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
