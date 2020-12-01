import argparse
import json
import time

from karton.core.__version__ import __version__
from karton.core.base import KartonServiceBase
from karton.core.config import Config
from karton.core.task import Task, TaskPriority, TaskState
from karton.core.utils import GracefulKiller


METRICS_ASSIGNED = "karton.metrics.assigned"
METRICS_GARBAGE_COLLECTED = "karton.metrics.garbage-collected"


class SystemService(KartonServiceBase):
    """
    Karton message broker.
    """
    identity = "karton.system"
    version = __version__

    GC_INTERVAL = 3 * 60
    TASK_DISPATCHED_TIMEOUT = 24 * 3600
    TASK_STARTED_TIMEOUT = 24 * 3600

    def __init__(self, config):
        super(SystemService, self).__init__(config=config)
        self.last_gc_trigger = 0
        self.shutdown = False
        self.killer = GracefulKiller(self.graceful_shutdown)

    def graceful_shutdown(self):
        self.log.info("Gracefully shutting down!")
        self.shutdown = True

    def gc_list_all_resources(self):
        bucket_name = self.config.minio_config["bucket"]
        return [
            (bucket_name, object.object_name)
            for object in self.minio.list_objects(bucket_name=bucket_name)
        ]

    def gc_list_all_tasks(self):
        return list(filter(None, [
            Task.unserialize(self.rs.get(task_key))
            for task_key in self.rs.keys("karton.task:*")
        ]))

    def gc_collect_resources(self):
        resources = set(self.gc_list_all_resources())
        tasks = self.gc_list_all_tasks()
        for task in tasks:
            for _, resource in task.iterate_resources():
                # If resource is referenced by task: remove it from set
                if (resource.bucket, resource.uid) in resources:
                    resources.remove((resource.bucket, resource.uid))
        for bucket_name, object_name in list(resources):
            try:
                self.minio.remove_object(bucket_name, object_name)
                self.log.debug("GC: Removed unreferenced resource %s:%s", bucket_name, object_name)
            except Exception:
                self.log.exception("GC: Error during resource removing %s:%s", bucket_name, object_name)

    def gc_collect_tasks(self):
        root_tasks = set()
        running_root_tasks = set()
        tasks = self.gc_list_all_tasks()
        enqueued_tasks = self.rs.lrange("karton.tasks", 0, -1)
        current_time = time.time()
        for task in tasks:
            root_tasks.add(task.root_uid)
            will_delete = False
            if (
                    task.status == TaskState.DECLARED and
                    task.uid not in enqueued_tasks and
                    task.last_update is not None and
                    current_time > task.last_update + self.TASK_DISPATCHED_TIMEOUT
            ):
                will_delete = True
                self.log.warning("Task %s is in Dispatched state more than %d seconds. Killed. (origin: %s)",
                                 task.uid, self.TASK_DISPATCHED_TIMEOUT, task.headers.get("origin", "<unknown>"))
            elif (
                    task.status == TaskState.STARTED and
                    task.last_update is not None and
                    current_time > task.last_update + self.TASK_STARTED_TIMEOUT
            ):
                # todo: Asynchronic tasks are just dispatched to another (system) queue
                # todo: Maybe these asynchronic things are just bad idea?
                will_delete = True
                self.log.warning("Task %s is in Started state more than %d seconds. Killed. (receiver: %s)",
                                 task.uid, self.TASK_STARTED_TIMEOUT, task.headers.get("receiver", "<unknown>"))
            elif task.status == TaskState.FINISHED:
                will_delete = True
                self.log.debug("GC: Finished task %s", task.uid)
            if will_delete:
                self.rs.delete("karton.task:" + task.uid)
                receiver = task.headers.get("receiver", "unknown")
                self.rs.hincrby(METRICS_GARBAGE_COLLECTED, receiver, 1)
            else:
                running_root_tasks.add(task.root_uid)
        for finished_root_task in root_tasks.difference(running_root_tasks):
            # TODO: Notification needed
            self.log.debug("GC: Finished root task %s", finished_root_task)

    def gc_collect(self):
        if time.time() > (self.last_gc_trigger + self.GC_INTERVAL):
            try:
                self.gc_collect_tasks()
                self.gc_collect_resources()
            except Exception:
                self.log.exception("GC: Exception during garbage collection")
            self.last_gc_trigger = time.time()

    def process_task(self, task):
        bound_identities = set()

        for client in self.rs.client_list():
            bound_identities.add(client["name"])

        self.log.info("[%s] Processing task %s", task.root_uid, task.uid)

        for identity, raw_binds in self.rs.hgetall("karton.binds").items():
            # For each identity
            binds = json.loads(raw_binds)
            if isinstance(binds, list):
                # v2.2.0 compatibility
                filters = binds
                persistent = not identity.endswith(".test")
            else:
                filters = binds["filters"]
                persistent = binds["persistent"]

            if identity not in bound_identities and not persistent:
                # If unbound and not persistent
                for queue in [
                    identity,  # Backwards compatibility, remove after upgrade
                    "karton.queue.{}:{}".format(TaskPriority.HIGH, identity),
                    "karton.queue.{}:{}".format(TaskPriority.NORMAL, identity),
                    "karton.queue.{}:{}".format(TaskPriority.LOW, identity)
                ]:
                    self.log.info("Non-persistent: unwinding tasks from queue %s", queue)
                    pipe = self.rs.pipeline()
                    pipe.lrange(queue, 0, -1)
                    pipe.delete(queue)
                    results = pipe.execute()
                    for unwound_task_uid in results[0]:
                        unwound_task_body = self.rs.get("karton.task:"+unwound_task_uid)
                        unwound_task = Task.unserialize(unwound_task_body)
                        unwound_task.last_update = time.time()
                        unwound_task.status = TaskState.FINISHED
                        self.log.info("Unwinding task %s", str(unwound_task.uid))
                        self.rs.set("karton.task:" + unwound_task.uid, unwound_task.serialize())
                        self.declare_task_state(unwound_task, TaskState.FINISHED, identity=identity)
                self.log.info("Non-persistent: removing bind %s", identity)
                self.rs.hdel("karton.binds", identity)
                # Continue with next identity
                continue

            for bind in filters:
                if task.matches_bind(bind):
                    routed_task = task.fork_task()
                    routed_task.status = TaskState.SPAWNED
                    routed_task.last_update = time.time()
                    routed_task.headers.update({"receiver": identity})
                    routed_task_body = routed_task.serialize()
                    self.rs.set("karton.task:" + routed_task.uid, routed_task_body)
                    self.rs.rpush("karton.queue.{}:{}".format(task.priority, identity), routed_task.uid)
                    self.rs.hincrby(METRICS_ASSIGNED, identity, 1)
                    self.declare_task_state(routed_task, TaskState.SPAWNED, identity=identity)
                    # Matched at least one bind: go to next identity
                    break

    def loop(self):
        self.log.info("Manager {} started".format(self.identity))

        while not self.shutdown:
            # order does matter! task dispatching must be before karton.operations to avoid races
            # Timeout must be shorter than GC_INTERVAL, but not too long allowing graceful shutdown
            data = self.rs.blpop(
                ["karton.tasks", "karton.operations"],
                timeout=5,
            )

            if data:
                queue, body = data
                if not isinstance(body, str):
                    body = body.decode("utf-8")
                if queue == "karton.tasks":
                    task = Task.unserialize(self.rs.get("karton.task:" + body))
                    self.process_task(task)
                    task.last_update = time.time()
                    task.status = TaskState.FINISHED
                    self.rs.set("karton.task:" + task.uid, task.serialize())
                elif queue == "karton.operations":
                    operation_body = json.loads(body)
                    task = Task.unserialize(operation_body["task"])
                    if task.status != operation_body["status"]:
                        task.last_update = time.time()
                        task.status = operation_body["status"]
                        self.log.info("[%s] %s %s task %s",
                                      str(task.root_uid),
                                      operation_body["identity"],
                                      operation_body["status"],
                                      str(task.uid))
                        self.rs.set("karton.task:" + task.uid, task.serialize())
                    # Pass new operation status to log
                    self.rs.lpush("karton.logs", body)
            self.gc_collect()

    @classmethod
    def args_parser(cls):
        parser = super().args_parser()
        parser.add_argument("--setup-bucket", action="store_true", help="Create missing bucket in MinIO")
        return parser

    @classmethod
    def main(cls):
        parser = cls.args_parser()
        args = parser.parse_args()

        config = Config(args.config_file)
        service = SystemService(config)

        bucket_name = config.minio_config["bucket"]

        if not service.minio.bucket_exists(bucket_name):
            if args.setup_bucket:
                service.log.info(
                    "Bucket %s is missing. Creating new one...",
                    bucket_name
                )
                service.minio.make_bucket(bucket_name)
            else:
                service.log.error(
                    "Bucket %s is missing! If you're sure that name is correct and "
                    "you don't want to create it manually, use --setup-bucket option",
                    bucket_name
                )
                return

        service.loop()
