import enum
import json
from collections import defaultdict, namedtuple
from io import BytesIO
from typing import Any, BinaryIO, Dict, Iterator, List, Optional, Tuple, Union

from minio import Minio
from redis import StrictRedis
from urllib3.response import HTTPResponse

from .task import Task, TaskPriority, TaskState

KARTON_TASKS_QUEUE = "karton.tasks"
KARTON_OPERATIONS_QUEUE = "karton.operations"
KARTON_LOG_CHANNEL = "karton.log"
KARTON_BINDS_HSET = "karton.binds"
KARTON_TASK_NAMESPACE = "karton.task"


KartonBind = namedtuple(
    "KartonBind", ["identity", "info", "version", "persistent", "filters"]
)


class KartonMetrics(enum.Enum):
    TASK_PRODUCED = "karton.metrics.produced"
    TASK_CONSUMED = "karton.metrics.consumed"
    TASK_CRASHED = "karton.metrics.crashed"
    TASK_ASSIGNED = "karton.metrics.assigned"
    TASK_GARBAGE_COLLECTED = "karton.metrics.garbage-collected"


class KartonBackend:
    def __init__(self, config):
        self.config = config
        self.redis = StrictRedis(
            host=config["redis"]["host"],
            port=int(config["redis"].get("port", 6379)),
            decode_responses=True,
        )
        self.minio = Minio(
            config["minio"]["address"],
            access_key=config["minio"]["access_key"],
            secret_key=config["minio"]["secret_key"],
            secure=bool(int(config["minio"].get("secure", True))),
        )

    @property
    def default_bucket_name(self) -> str:
        return self.config.minio_config["bucket"]

    @staticmethod
    def get_queue_name(identity: str, priority: TaskPriority) -> str:
        """
        Return Redis routed task queue name for given identity and priority

        :param identity: Karton service identity
        :param priority: Queue priority (TaskPriority enum value)
        :return: Queue name
        """
        return f"karton.queue.{priority.value}:{identity}"

    @staticmethod
    def get_queue_names(identity: str) -> List[str]:
        """
        Return all Redis routed task queue names for given identity,
        ordered by priority (descending). Used internally by Consumer.

        :param identity: Karton service identity
        :return: List of queue names
        """
        return [
            identity,  # Backwards compatibility (2.x.x)
            KartonBackend.get_queue_name(identity, TaskPriority.HIGH),
            KartonBackend.get_queue_name(identity, TaskPriority.NORMAL),
            KartonBackend.get_queue_name(identity, TaskPriority.LOW),
        ]

    @staticmethod
    def serialize_bind(bind: KartonBind) -> str:
        """
        Serialize KartonBind object (Karton service registration)

        :param bind: KartonBind object with bind definition
        :return: Serialized bind data
        """
        return json.dumps(
            {
                "info": bind.info,
                "version": bind.version,
                "filters": bind.filters,
                "persistent": bind.persistent,
            },
            sort_keys=True,
        )

    @staticmethod
    def unserialize_bind(identity: str, bind_data: str) -> KartonBind:
        """
        Deserialize KartonBind object for given identity.
        Compatible with Karton 2.x.x and 3.x.x

        :param identity: Karton service identity
        :param bind_data: Serialized bind data
        :return: KartonBind object with bind definition
        """
        bind = json.loads(bind_data)
        if isinstance(bind, list):
            # Backwards compatibility (v2.x.x)
            return KartonBind(
                identity=identity,
                info=None,
                version="2.x.x",
                persistent=not identity.endswith(".test"),
                filters=bind,
            )
        return KartonBind(
            identity=identity,
            info=bind["info"],
            version=bind["version"],
            persistent=bind["persistent"],
            filters=bind["filters"],
        )

    def get_bind(self, identity: str) -> KartonBind:
        """
        Get bind object for given identity

        :param identity: Karton service identity
        :return: KartonBind object
        """
        return self.unserialize_bind(
            identity, self.redis.hget(KARTON_BINDS_HSET, identity)
        )

    def get_binds(self) -> List[KartonBind]:
        """
        Get all binds registered in Redis

        :return: List of KartonBind objects for subsequent identities
        """
        return [
            self.unserialize_bind(identity, raw_bind)
            for identity, raw_bind in self.redis.hgetall(KARTON_BINDS_HSET).items()
        ]

    def register_bind(self, bind: KartonBind) -> Optional[KartonBind]:
        """
        Register bind for Karton service and return the old one

        :param bind: KartonBind object with bind definition
        :return: Old KartonBind that was registered under this identity
        """
        with self.redis.pipeline(transaction=True) as pipe:
            pipe.hget(KARTON_BINDS_HSET, bind.identity)
            pipe.hset(KARTON_BINDS_HSET, bind.identity, self.serialize_bind(bind))
            old_serialized_bind, _ = pipe.execute()

        if old_serialized_bind:
            return self.unserialize_bind(bind.identity, old_serialized_bind)
        else:
            return None

    def unregister_bind(self, identity: str) -> None:
        """
        Removes bind for identity
        :param bind: Identity to be unregistered
        """
        self.redis.hdel(KARTON_BINDS_HSET, identity)

    def set_consumer_identity(self, identity: str) -> None:
        """
        Sets identity for current Redis connection
        """
        return self.redis.client_setname(identity)

    def get_online_consumers(self) -> Dict[str, List[str]]:
        """
        Gets all online consumer identities

        :return: Dictionary {identity: [list of clients]}
        """
        bound_identities = defaultdict(list)
        for client in self.redis.client_list():
            bound_identities[client["name"]].append(client)
        return bound_identities

    def get_task(self, task_uid: str) -> Optional[Task]:
        """
        Get task object with given identifier

        :param task_uid: Task identifier
        :return: Task object
        """
        task_data = self.redis.get(f"{KARTON_TASK_NAMESPACE}:{task_uid}")
        if not task_data:
            return None
        return Task.unserialize(task_data, backend=self)

    def get_tasks(self, task_uid_list: List[str]) -> List[Task]:
        """
        Get multiple tasks for given identifier list

        :param task_uid_list: List of task identifiers
        :return: List of task objects
        """
        task_list = self.redis.mget(
            [f"{KARTON_TASK_NAMESPACE}:{task_uid}" for task_uid in task_uid_list]
        )
        return [
            Task.unserialize(task_data, backend=self)
            for task_data in task_list
            if task_data is not None
        ]

    def get_all_tasks(self) -> List[Task]:
        """
        Get all tasks registered in Redis

        :return: List with Task objects
        """
        tasks = self.redis.keys(f"{KARTON_TASK_NAMESPACE}:*")
        return [
            Task.unserialize(task_data)
            for task_data in self.redis.mget(tasks)
            if task_data is not None
        ]

    def register_task(self, task: Task) -> None:
        """
        Register task in Redis.

        Consumer should register only Declared tasks.
        Status change should be done using set_task_status.

        :param task: Task object
        """
        self.redis.set(f"{KARTON_TASK_NAMESPACE}:{task.uid}", task.serialize())

    def set_task_status(
        self, task: Task, status: TaskState, consumer: Optional[str] = None
    ) -> None:
        """
        Request task status change to be applied by karton-system

        :param task: Task object
        :param status: New task status (TaskState)
        :param consumer: Consumer identity
        """
        self.redis.rpush(
            KARTON_OPERATIONS_QUEUE,
            json.dumps(
                {
                    "status": status.value,
                    "identity": consumer,
                    "task": task.serialize(),
                    "type": "operation",
                }
            ),
        )

    def delete_task(self, task: Task) -> None:
        """
        Remove task from Redis

        :param task: Task object
        """
        self.redis.delete(f"{KARTON_TASK_NAMESPACE}:{task.uid}")

    def get_task_queue(self, queue: str) -> List[Task]:
        """
        Return all tasks in provided queue

        :param queue: Queue name
        :return: List with Task objects contained in queue
        """
        task_uids = self.redis.lrange(queue, 0, -1)
        return self.get_tasks(task_uids)

    def get_task_ids_from_queue(self, queue: str) -> List[str]:
        """
        Return all task UIDs in a queue

        :param queue: Queue name
        :return: List with task identifiers contained in queue
        """
        return self.redis.lrange(queue, 0, -1)

    def remove_task_queue(self, queue: str) -> List[Task]:
        """
        Remove task queue with all contained tasks

        :param queue: Queue name
        :return: List with Task objects contained in queue
        """
        pipe = self.redis.pipeline()
        pipe.lrange(queue, 0, -1)
        pipe.delete(queue)
        return self.get_tasks(pipe.execute()[0])

    def produce_unrouted_task(self, task: Task) -> None:
        """
        Add given task to unrouted task (``karton.tasks``) queue

        Task must be registered before with :py:meth:`register_task`

        :param task: Task object
        """
        self.redis.rpush(KARTON_TASKS_QUEUE, task.uid)

    def produce_routed_task(self, identity: str, task: Task) -> None:
        """
        Add given task to routed task queue of given identity

        Task must be registered using :py:meth:`register_task`

        :param identity: Karton service identity
        :param task: Task object
        """
        self.redis.rpush(self.get_queue_name(identity, task.priority), task.uid)

    def consume_queues(
        self, queues: Union[str, List[str]], timeout: int = 0
    ) -> Optional[Tuple[str, str]]:
        """
        Get item from queues (ordered from the most to the least prioritized)
        If there are no items, wait until one appear.

        :param queues: Redis queue name or list of names
        :param timeout: Waiting for item timeout (default: 0 = wait forever)
        :return: Tuple of [queue_name, item] objects or None if timeout has been reached
        """
        return self.redis.blpop(queues, timeout=timeout)

    def consume_routed_task(self, identity: str, timeout: int = 5) -> Optional[Task]:
        """
        Get routed task for given consumer identity.

        If there are no tasks, blocks until new one appears or timeout is reached.

        :param identity: Karton service identity
        :param timeout: Waiting for task timeout (default: 5)
        :return: Task object
        """
        item = self.consume_queues(
            self.get_queue_names(identity),
            timeout=timeout,
        )
        if not item:
            return None
        queue, data = item
        return self.get_task(data)

    @staticmethod
    def _log_channel(logger_name: Optional[str], level: Optional[str]) -> str:
        return ".".join(
            [KARTON_LOG_CHANNEL, (level or "*").lower(), logger_name or "*"]
        )

    def produce_log(
        self,
        log_record: Dict[str, Any],
        logger_name: str,
        level: str,
    ) -> bool:
        """
        Push new log record to the logs channel

        :param log_record: Dict with log record
        :param logger_name: Logger name
        :param level: Log level
        :return: True if any active log consumer received log record
        """
        return (
            self.redis.publish(
                self._log_channel(logger_name, level), json.dumps(log_record)
            )
            > 0
        )

    def consume_log(
        self,
        timeout: int = 5,
        logger_filter: Optional[str] = None,
        level: Optional[str] = None,
    ) -> Iterator[Optional[Dict[str, Any]]]:
        """
        Subscribe to logs channel and yield subsequent log records
        or None if timeout has been reached.

        If you want to subscribe only to a specific logger name
        and/or log level, pass them via logger_filter and level arguments.

        :param timeout: Waiting for log record timeout (default: 5)
        :param logger_filter: Filter for name of consumed logger
        :param level: Log level
        :return: Dict with log record
        """
        with self.redis.pubsub() as pubsub:
            pubsub.psubscribe(self._log_channel(logger_filter, level))
            while pubsub.subscribed:
                item = pubsub.get_message(
                    ignore_subscribe_messages=True, timeout=timeout
                )
                if item and item["type"] == "message":
                    body = json.loads(item["data"])
                    if "task" in body and isinstance(body["task"], str):
                        body["task"] = json.loads(body["task"])
                    yield body
                yield None

    def increment_metrics(self, metric: KartonMetrics, identity: str) -> None:
        """
        Increments metrics for given operation type and identity

        :param metric: Operation metric type
        :param identity: Related Karton service identity
        """
        self.redis.hincrby(metric.value, identity, 1)

    def upload_object(
        self,
        bucket: str,
        object_uid: str,
        content: Union[bytes, BinaryIO],
        length: int = None,
    ) -> None:
        """
        Upload resource object to underlying object storage (Minio)

        :param bucket: Bucket name
        :param object_uid: Object identifier
        :param content: Object content as bytes or file-like stream
        :param length: Object content length (if file-like object provided)
        """
        if isinstance(content, bytes):
            length = len(content)
            content = BytesIO(content)
        self.minio.put_object(bucket, object_uid, content, length)

    def upload_object_from_file(self, bucket: str, object_uid: str, path: str) -> None:
        """
        Upload resource object file to underlying object storage

        :param bucket: Bucket name
        :param object_uid: Object identifier
        :param path: Path to the object content
        """
        self.minio.fput_object(bucket, object_uid, path)

    def get_object(self, bucket: str, object_uid: str) -> HTTPResponse:
        """
        Get resource object stream with the content.

        Returned response should be closed after use to release network resources.
        To reuse the connection, it's required to call `response.release_conn()`
        explicitly.

        :param bucket: Bucket name
        :param object_uid: Object identifier
        :return: Response object with content
        """
        return self.minio.get_object(bucket, object_uid)

    def download_object(self, bucket: str, object_uid: str) -> bytes:
        """
        Download resource object from object storage.

        :param bucket: Bucket name
        :param object_uid: Object identifier
        :return: Content bytes
        """
        reader = self.minio.get_object(bucket, object_uid)
        try:
            return reader.read()
        finally:
            reader.release_conn()
            reader.close()

    def download_object_to_file(self, bucket: str, object_uid: str, path: str) -> None:
        """
        Download resource object from object storage to file

        :param bucket: Bucket name
        :param object_uid: Object identifier
        :param path: Target file path
        """
        self.minio.fget_object(bucket, object_uid, path)

    def list_objects(self, bucket: str) -> List[str]:
        """
        List identifiers of stored resource objects

        :param bucket: Bucket name
        :return: List of object identifiers
        """
        return [object.object_name for object in self.minio.list_objects(bucket)]

    def remove_object(self, bucket: str, object_uid: str) -> None:
        """
        Remove resource object from object storage

        :param bucket: Bucket name
        :param object_uid: Object identifier
        """
        self.minio.remove_object(bucket, object_uid)

    def check_bucket_exists(self, bucket: str, create: bool = False) -> bool:
        """
        Check if bucket exists and optionally create it if it doesn't.

        :param bucket: Bucket name
        :param create: Create bucket if doesn't exist
        :return: True if bucket exists yet
        """
        if self.minio.bucket_exists(bucket):
            return True
        if create:
            self.minio.make_bucket(bucket)
        return False
