from collections import defaultdict, namedtuple
import json

from redis import StrictRedis
from minio import Minio

from .task import Task, TaskPriority


KARTON_TASKS_QUEUE = "karton.tasks"
KARTON_OPERATIONS_QUEUE = "karton.operations"
KARTON_LOGS_QUEUE = "karton.logs"
KARTON_BINDS_HSET = "karton.binds"
KARTON_TASK_NAMESPACE = "karton.task"


KartonBind = namedtuple(
    "KartonBind", ["identity", "info", "version", "persistent", "filters"]
)


class KartonBackend:
    def __init__(self, config):
        self.redis = StrictRedis(host=config["redis"]["host"],
                                 port=int(config["redis"].get("port", 6379)),
                                 decode_responses=True)
        self.minio = Minio(
            config["minio"]["address"],
            access_key=config["minio"]["access_key"],
            secret_key=config["minio"]["secret_key"],
            secure=bool(int(config["minio"].get("secure", True))),
        )

    @staticmethod
    def get_queue_name(identity, priority):
        """
        Return Redis routed task queue name for given identity and priority

        :param identity: Karton service identity
        :param priority: Queue priority (TaskPriority enum value)
        """
        return f"karton.queue.{priority}:{identity}"

    @staticmethod
    def get_queue_names(identity):
        """
        Return all Redis routed task queue names for given identity,
        ordered by priority (descending). Used internally by Consumer.

        :param identity: Karton service identity
        """
        return [
            identity,  # Backwards compatibility (2.x.x)
            KartonBackend.get_queue_name(identity, TaskPriority.HIGH),
            KartonBackend.get_queue_name(identity, TaskPriority.NORMAL),
            KartonBackend.get_queue_name(identity, TaskPriority.LOW)
        ]

    @staticmethod
    def serialize_bind(bind):
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
                "persistent": bind.persistent
            },
            sort_keys=True
        )

    @staticmethod
    def unserialize_bind(identity, bind_data):
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
                filters=bind
            )
        return KartonBind(
            identity=identity,
            info=bind["info"],
            version=bind["version"],
            persistent=bind["persistent"],
            filters=bind["filters"]
        )

    def get_bind(self, identity):
        """
        Get bind object for given identity

        :param identity: Karton service identity
        :return: KartonBind object
        """
        return self.unserialize_bind(identity, self.redis.hget(KARTON_BINDS_HSET, identity))

    def get_binds(self):
        """
        Get all binds registered in Redis

        :return: List of KartonBind objects for subsequent identities
        """
        return [
            self.unserialize_bind(identity, raw_bind)
            for identity, raw_bind in self.redis.hgetall(KARTON_BINDS_HSET).items()
        ]

    def register_bind(self, bind):
        """
        Register bind for Karton service and return the old one

        :param bind: KartonBind object with bind definition
        """
        with self.redis.pipeline(transaction=True) as pipe:
            pipe.hget(KARTON_BINDS_HSET, bind.identity)
            pipe.hset(KARTON_BINDS_HSET, bind.identity, self.serialize_bind(bind))
            old_serialized_bind, _ = pipe.execute()

        if old_serialized_bind:
            return self.unserialize_bind(bind.identity, old_serialized_bind)
        else:
            return None

    def unregister_bind(self, identity):
        """
        Removes bind for identity
        """
        self.redis.hdel(KARTON_BINDS_HSET, identity)

    def set_consumer_identity(self, identity):
        """
        Sets identity for current Redis connection
        """
        return self.redis.client_setname(identity)

    def get_online_consumers(self):
        """
        Gets all online consumer identities

        :return: Dictionary {identity: [list of clients]}
        """
        bound_identities = defaultdict(list)
        for client in self.redis.client_list():
            bound_identities[client["name"]].append(client)
        return bound_identities

    def get_task(self, task_uid):
        """
        Get task object with given identifier

        :param task_uid: Task identifier
        :return: Task object
        """
        task_data = self.redis.get(f"{KARTON_TASK_NAMESPACE}:{task_uid}")
        if not task_data:
            return None
        return Task.unserialize(task_data, minio=self.minio)

    def get_tasks(self, task_uid_list):
        """
        Get multiple tasks for given identifier list

        :param task_uid_list: List of task identifiers
        :return: List of task objects
        """
        task_list = self.redis.mget([
            f"{KARTON_TASK_NAMESPACE}:{task_uid}"
            for task_uid in task_uid_list
        ])
        return [
            Task.unserialize(task_data, minio=self.minio)
            for task_data in task_list
            if task_data is not None
        ]

    def get_all_tasks(self):
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

    def register_task(self, task):
        """
        Register task in Redis.

        Consumer should register only Declared tasks.
        Status change should be done using set_task_status.

        :param task: Task object
        """
        self.redis.set(f"{KARTON_TASK_NAMESPACE}:{task.uid}", task.serialize())

    def set_task_status(self, task, status, consumer=None):
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
                    "status": status,
                    "identity": consumer,
                    "task": task.serialize(),
                    "type": "operation",
                }
            ),
        )

    def delete_task(self, task):
        """
        Remove task from Redis

        :param task: Task object
        """
        self.redis.delete(f"{KARTON_TASK_NAMESPACE}:{task.uid}")

    def get_task_queue(self, queue):
        """
        Return all tasks in provided queue

        :param queue: Queue name
        :return: List with Task objects contained in queue
        """
        return [
            self.get_task(uid)
            for uid in self.redis.lrange(queue, 0, -1)
        ]

    def get_task_ids_from_queue(self, queue):
        """
        Return all task UIDs in queue

        :param queue: Queue name
        :return: Iterator with task identifiers contained in queue
        """
        return self.redis.lrange(queue, 0, -1)

    def remove_task_queue(self, queue):
        """
        Remove task queue with all contained tasks

        :param queue: Queue name
        :return: List with Task objects contained in queue
        """
        pipe = self.redis.pipeline()
        pipe.lrange(queue, 0, -1)
        pipe.delete(queue)
        return self.get_tasks(pipe.execute()[0])

    def produce_unrouted_task(self, task):
        """
        Add given task to unrouted task (``karton.tasks``) queue

        Task must be registered before with :py:meth:`register_task`

        :param task: Task object
        """
        self.redis.rpush(KARTON_TASKS_QUEUE, task.uid)

    def produce_routed_task(self, identity, task):
        """
        Add given task to routed task queue of given identity

        Task must be registered using :py:meth:`register_task`

        :param identity: Karton service identity
        :param task: Task object
        """
        self.redis.rpush(
            self.get_queue_name(identity, task.priority),
            task.uid
        )

    def consume_queues(self, queues, timeout=0):
        """
        Get item from queues (ordered from the most to the least prioritized)
        If there are no items, wait until one appear.

        :param queues: Redis queue name or list of names
        :param timeout: Waiting for item timeout (default: 0 = wait forever)
        :return: Tuple of [queue_name, item] objects or None if timeout has been reached
        """
        return self.redis.blpop(queues, timeout=timeout)

    def consume_routed_task(self, identity, timeout=5):
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
