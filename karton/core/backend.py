import enum
import json
import time
import warnings
from collections import defaultdict, namedtuple
from typing import IO, Any, Dict, Iterable, Iterator, List, Optional, Set, Tuple, Union

import boto3
from redis import AuthenticationError, StrictRedis
from redis.client import Pipeline
from urllib3.response import HTTPResponse

from .task import Task, TaskPriority, TaskState
from .utils import chunks

KARTON_TASKS_QUEUE = "karton.tasks"
KARTON_OPERATIONS_QUEUE = "karton.operations"
KARTON_LOG_CHANNEL = "karton.log"
KARTON_BINDS_HSET = "karton.binds"
KARTON_TASK_NAMESPACE = "karton.task"
KARTON_OUTPUTS_NAMESPACE = "karton.outputs"


KartonBind = namedtuple(
    "KartonBind",
    ["identity", "info", "version", "persistent", "filters", "service_version"],
)


KartonOutputs = namedtuple("KartonOutputs", ["identity", "outputs"])


class KartonMetrics(enum.Enum):
    TASK_PRODUCED = "karton.metrics.produced"
    TASK_CONSUMED = "karton.metrics.consumed"
    TASK_CRASHED = "karton.metrics.crashed"
    TASK_ASSIGNED = "karton.metrics.assigned"
    TASK_GARBAGE_COLLECTED = "karton.metrics.garbage-collected"


class KartonBackend:
    def __init__(self, config, identity: Optional[str] = None) -> None:
        self.config = config
        self.identity = identity
        self.redis = self.make_redis(config, identity=identity)
        self.s3 = boto3.client(
            "s3",
            endpoint_url=config["s3"]["address"],
            aws_access_key_id=config["s3"]["access_key"],
            aws_secret_access_key=config["s3"]["secret_key"],
        )

    def make_redis(self, config, identity: Optional[str] = None) -> StrictRedis:
        """
        Create and test a Redis connection.

        :param config: The karton configuration
        :param identity: Karton service identity
        :return: Redis connection
        """
        redis_args = {
            "host": config["redis"]["host"],
            "port": config.getint("redis", "port", 6379),
            "db": config.getint("redis", "db", 0),
            "username": config.get("redis", "username"),
            "password": config.get("redis", "password"),
            "client_name": identity,
            # set socket_timeout to None if set to 0
            "socket_timeout": config.getint("redis", "socket_timeout", 30) or None,
            "decode_responses": True,
        }
        try:
            redis = StrictRedis(**redis_args)
            redis.ping()
        except AuthenticationError:
            # Maybe we've sent a wrong password.
            # Or maybe the server is not (yet) password protected
            # To make smooth transition possible, try to login insecurely
            del redis_args["password"]
            redis = StrictRedis(**redis_args)
            redis.ping()
        return redis

    @property
    def default_bucket_name(self) -> str:
        bucket_name = self.config.get("s3", "bucket")
        if not bucket_name:
            raise RuntimeError("S3 default bucket is not defined in configuration")
        return bucket_name

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
                "service_version": bind.service_version,
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
                service_version=None,
            )
        return KartonBind(
            identity=identity,
            info=bind["info"],
            version=bind["version"],
            persistent=bind["persistent"],
            filters=bind["filters"],
            service_version=bind.get("service_version"),
        )

    @staticmethod
    def unserialize_output(identity: str, output_data: Set[str]) -> KartonOutputs:
        """
        Deserialize KartonOutputs object for given identity.

        :param identity: Karton service identity
        :param output_data: Serialized output data
        :return: KartonOutputs object with outputs definition
        """
        output = [json.loads(output_type) for output_type in output_data]
        return KartonOutputs(identity=identity, outputs=output)

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

    def set_consumer_identity(self, _: str) -> None:
        """
        Sets identity for current Redis connection
        """
        warnings.warn(
            "set_consumer_identity is deprecated and does nothing from v4.5.0. "
            "Use identity constructor argument instead",
            DeprecationWarning,
        )

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

    def get_tasks(self, task_uid_list: List[str], chunk_size: int = 1000) -> List[Task]:
        """
        Get multiple tasks for given identifier list

        :param task_uid_list: List of task identifiers
        :param chunk_size: Size of chunks passed to the Redis MGET command
        :return: List of task objects
        """
        keys = chunks(
            [f"{KARTON_TASK_NAMESPACE}:{task_uid}" for task_uid in task_uid_list],
            chunk_size,
        )
        return [
            Task.unserialize(task_data, backend=self)
            for chunk in keys
            for task_data in self.redis.mget(chunk)
            if task_data is not None
        ]

    def get_all_tasks(self, chunk_size: int = 1000) -> List[Task]:
        """
        Get all tasks registered in Redis

        :param chunk_size: Size of chunks passed to the Redis MGET command
        :return: List with Task objects
        """
        tasks = self.redis.keys(f"{KARTON_TASK_NAMESPACE}:*")
        return [
            Task.unserialize(task_data)
            for chunk in chunks(tasks, chunk_size)
            for task_data in self.redis.mget(chunk)
            if task_data is not None
        ]

    def register_task(self, task: Task, pipe: Optional[Pipeline] = None) -> None:
        """
        Register or update task in Redis.

        :param task: Task object
        :param pipe: Optional pipeline object if operation is a part of pipeline
        """
        rs = pipe or self.redis
        rs.set(f"{KARTON_TASK_NAMESPACE}:{task.uid}", task.serialize())

    def register_tasks(self, tasks: List[Task]) -> None:
        """
        Register or update multiple tasks in Redis.
        :param tasks: List of task objects
        """
        taskmap = {
            f"{KARTON_TASK_NAMESPACE}:{task.uid}": task.serialize() for task in tasks
        }
        self.redis.mset(taskmap)

    def set_task_status(
        self, task: Task, status: TaskState, pipe: Optional[Pipeline] = None
    ) -> None:
        """
        Request task status change to be applied by karton-system

        :param task: Task object
        :param status: New task status (TaskState)
        :param pipe: Optional pipeline object if operation is a part of pipeline
        """
        if task.status == status:
            return
        task.status = status
        task.last_update = time.time()
        self.register_task(task, pipe=pipe)

    def delete_task(self, task: Task) -> None:
        """
        Remove task from Redis

        :param task: Task object
        """
        self.redis.delete(f"{KARTON_TASK_NAMESPACE}:{task.uid}")

    def delete_tasks(self, tasks: Iterable[Task], chunk_size: int = 1000) -> None:
        """
        Remove multiple tasks from Redis

        :param tasks: List of Task objects
        :param chunk_size: Size of chunks passed to the Redis DELETE command
        """
        keys = [f"{KARTON_TASK_NAMESPACE}:{task.uid}" for task in tasks]
        for chunk in chunks(keys, chunk_size):
            self.redis.delete(*chunk)

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

    def produce_routed_task(
        self, identity: str, task: Task, pipe: Optional[Pipeline] = None
    ) -> None:
        """
        Add given task to routed task queue of given identity

        Task must be registered using :py:meth:`register_task`

        :param identity: Karton service identity
        :param task: Task object
        :param pipe: Optional pipeline object if operation is a part of pipeline
        """
        rs = pipe or self.redis
        rs.rpush(self.get_queue_name(identity, task.priority), task.uid)

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

    def consume_queues_batch(self, queue: str, max_count: int) -> List[str]:
        """
        Get a batch of items from the queue

        :param queue: Redis queue name
        :param max_count: Maximum batch count
        """
        p = self.redis.pipeline(transaction=True)
        p.lrange(queue, 0, max_count - 1)
        p.ltrim(queue, max_count, -1)
        return p.execute()[0]

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

    def produce_logs(
        self,
        log_records: List[Dict[str, Any]],
        logger_name: str,
        level: str,
    ) -> None:
        """
        Push multiple log records to the logs channel

        :param log_records: List of dicts with log record
        :param logger_name: Logger name
        :param level: Log level
        """
        p = self.redis.pipeline()
        channel = self._log_channel(logger_name, level)
        for log_record in log_records:
            p.publish(channel, json.dumps(log_record))
        p.execute()

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
                if item and item["type"] == "pmessage":
                    body = json.loads(item["data"])
                    if "task" in body and isinstance(body["task"], str):
                        body["task"] = json.loads(body["task"])
                    yield body
                yield None

    def increment_metrics(
        self, metric: KartonMetrics, identity: str, pipe: Optional[Pipeline] = None
    ) -> None:
        """
        Increments metrics for given operation type and identity

        :param metric: Operation metric type
        :param identity: Related Karton service identity
        :param pipe: Optional pipeline object if operation is a part of pipeline
        """
        rs = pipe or self.redis
        rs.hincrby(metric.value, identity, 1)

    def increment_metrics_list(
        self, metric: KartonMetrics, identities: List[str]
    ) -> None:
        """
        Increments metrics for multiple identities via single pipeline

        :param metric: Operation metric type
        :param identities: List of Karton service identities
        """
        p = self.redis.pipeline()
        for identity in identities:
            p.hincrby(metric.value, identity, 1)
        p.execute()

    def get_metrics(self, metric: KartonMetrics) -> Dict[str, int]:
        """
        Get a {karton-identity: current-number-of-tasks} mapping for a given metric.

        :param metric: Operation metric type
        """
        return {k: int(v) for k, v in self.redis.hgetall(metric.value).items()}

    def upload_object(
        self,
        bucket: str,
        object_uid: str,
        content: Union[bytes, IO[bytes]],
    ) -> None:
        """
        Upload resource object to underlying object storage (S3)

        :param bucket: Bucket name
        :param object_uid: Object identifier
        :param content: Object content as bytes or file-like stream
        """
        self.s3.put_object(Bucket=bucket, Key=object_uid, Body=content)

    def upload_object_from_file(self, bucket: str, object_uid: str, path: str) -> None:
        """
        Upload resource object file to underlying object storage

        :param bucket: Bucket name
        :param object_uid: Object identifier
        :param path: Path to the object content
        """
        with open(path, "rb") as f:
            self.s3.put_object(Bucket=bucket, Key=object_uid, Body=f)

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
        return self.s3.get_object(Bucket=bucket, Key=object_uid)["Body"]

    def download_object(self, bucket: str, object_uid: str) -> bytes:
        """
        Download resource object from object storage.

        :param bucket: Bucket name
        :param object_uid: Object identifier
        :return: Content bytes
        """
        with self.s3.get_object(Bucket=bucket, Key=object_uid)["Body"] as f:
            ret = f.read()
        return ret

    def download_object_to_file(self, bucket: str, object_uid: str, path: str) -> None:
        """
        Download resource object from object storage to file

        :param bucket: Bucket name
        :param object_uid: Object identifier
        :param path: Target file path
        """
        self.s3.download_file(Bucket=bucket, Key=object_uid, Filename=path)

    def list_objects(self, bucket: str) -> List[str]:
        """
        List identifiers of stored resource objects

        :param bucket: Bucket name
        :return: List of object identifiers
        """
        objs = list()
        paginator = self.s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket):
            for obj in page.get("Contents", list()):
                objs.append(obj["Key"])
        return objs

    def remove_object(self, bucket: str, object_uid: str) -> None:
        """
        Remove resource object from object storage

        :param bucket: Bucket name
        :param object_uid: Object identifier
        """
        self.s3.delete_object(Bucket=bucket, Key=object_uid)

    def remove_objects(self, bucket: str, object_uids: Iterable[str]) -> None:
        """
        Bulk remove resource objects from object storage

        :param bucket: Bucket name
        :param object_uids: Object identifiers
        """
        for delete_objects in chunks([{"Key": uid} for uid in object_uids], 1000):
            self.s3.delete_objects(Bucket=bucket, Delete={"Objects": delete_objects})

    def check_bucket_exists(self, bucket: str, create: bool = False) -> bool:
        """
        Check if bucket exists and optionally create it if it doesn't.

        :param bucket: Bucket name
        :param create: Create bucket if doesn't exist
        :return: True if bucket exists yet
        """
        try:
            self.s3.head_bucket(Bucket=bucket)
            return True
        except self.s3.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                if create:
                    self.s3.create_bucket(Bucket=bucket)
            else:
                raise e
        return False

    def log_identity_output(self, identity: str, headers: Dict[str, Any]) -> None:
        """
        Store the type of task outputted for given producer to
        be used in tracking karton service connections.

        :param identity: producer identity
        :param headers: outputted headers
        """

        self.redis.sadd(f"{KARTON_OUTPUTS_NAMESPACE}:{identity}", json.dumps(headers))
        self.redis.expire(f"{KARTON_OUTPUTS_NAMESPACE}:{identity}", 60 * 60 * 24 * 30)

    def get_outputs(self) -> List[KartonOutputs]:
        """
        Get a list of the output types for each karton.

        :return: List of KartonOutputs
        """

        output_keys = self.redis.keys(f"{KARTON_OUTPUTS_NAMESPACE}:*")
        return [
            self.unserialize_output(
                identity.split(":")[1], self.redis.smembers(identity)
            )
            for identity in output_keys
        ]

    def make_pipeline(self, transaction: bool = False) -> Pipeline:
        return self.redis.pipeline(transaction=transaction)
