import argparse
import json
import time
from typing import List, Optional

from karton.core.__version__ import __version__
from karton.core.backend import (
    KARTON_OPERATIONS_QUEUE,
    KARTON_TASKS_QUEUE,
    KartonBind,
    KartonMetrics,
)
from karton.core.base import KartonServiceBase
from karton.core.config import Config
from karton.core.task import Task, TaskState


class SystemService(KartonServiceBase):
    """
    Karton message broker.
    """

    identity = "karton.system"
    version = __version__

    GC_INTERVAL = 3 * 60
    TASK_DISPATCHED_TIMEOUT = 24 * 3600
    TASK_STARTED_TIMEOUT = 24 * 3600
    TASK_CRASHED_TIMEOUT = 3 * 24 * 3600

    def __init__(
        self,
        config: Optional[Config],
        enable_gc: bool = True,
        enable_router: bool = True,
        gc_interval: int = GC_INTERVAL,
    ) -> None:
        super(SystemService, self).__init__(config=config)
        self.last_gc_trigger = time.time()
        self.enable_gc = enable_gc
        self.enable_router = enable_router
        self.gc_interval = gc_interval

    def gc_collect_resources(self) -> None:
        # Collects unreferenced resources left in object storage
        karton_bucket = self.backend.default_bucket_name
        resources_to_remove = set(self.backend.list_objects(karton_bucket))
        # Note: it is important to get list of resources before getting list of tasks!
        # Task is created before resource upload to lock the reference to the resource.
        tasks = self.backend.get_all_tasks()
        for task in tasks:
            for _, resource in task.iterate_resources():
                # If resource is referenced by task: remove it from set
                if (
                    resource.bucket == karton_bucket
                    and resource.uid in resources_to_remove
                ):
                    resources_to_remove.remove(resource.uid)
        # Remove unreferenced resources
        if resources_to_remove:
            for err in self.backend.remove_objects(
                karton_bucket, list(resources_to_remove)
            ):
                self.log.error(err)

    def gc_collect_abandoned_queues(self):
        online_consumers = self.backend.get_online_consumers()
        for bind in self.backend.get_binds():
            identity = bind.identity
            if identity not in online_consumers and not bind.persistent:
                # If offline and not persistent: remove queue
                for queue in self.backend.get_queue_names(identity):
                    self.log.info(
                        "Non-persistent: unwinding tasks from queue %s", queue
                    )
                    removed_tasks = self.backend.remove_task_queue(queue)
                    for removed_task in removed_tasks:
                        self.log.info("Unwinding task %s", str(removed_task.uid))
                        # Mark task as finished
                        self.backend.set_task_status(removed_task, TaskState.FINISHED)
                    self.log.info("Non-persistent: removing bind %s", identity)
                    self.backend.unregister_bind(identity)

    def gc_collect_tasks(self) -> None:
        # Collects finished tasks
        root_tasks = set()
        running_root_tasks = set()
        tasks = self.backend.get_all_tasks()
        enqueued_task_uids = self.backend.get_task_ids_from_queue(KARTON_TASKS_QUEUE)

        current_time = time.time()
        to_delete = []

        for task in tasks:
            root_tasks.add(task.root_uid)
            if (
                task.status == TaskState.DECLARED
                and task.uid not in enqueued_task_uids
                and task.last_update is not None
                and current_time > task.last_update + self.TASK_DISPATCHED_TIMEOUT
            ):
                to_delete.append(task)
                self.log.warning(
                    "Task %s is in Dispatched state more than %d seconds. "
                    "Killed. (origin: %s)",
                    task.uid,
                    self.TASK_DISPATCHED_TIMEOUT,
                    task.headers.get("origin", "<unknown>"),
                )
            elif (
                task.status == TaskState.STARTED
                and task.last_update is not None
                and current_time > task.last_update + self.TASK_STARTED_TIMEOUT
            ):
                to_delete.append(task)
                self.log.warning(
                    "Task %s is in Started state more than %d seconds. "
                    "Killed. (receiver: %s)",
                    task.uid,
                    self.TASK_STARTED_TIMEOUT,
                    task.headers.get("receiver", "<unknown>"),
                )
            elif task.status == TaskState.FINISHED:
                to_delete.append(task)
                self.log.debug("GC: Finished task %s", task.uid)
            elif (
                task.status == TaskState.CRASHED
                and task.last_update is not None
                and current_time > task.last_update + self.TASK_CRASHED_TIMEOUT
            ):
                to_delete.append(task)
                self.log.debug(
                    "GC: Task %s is in Crashed state more than %d seconds. "
                    "Killed. (receiver: %s)",
                    task.uid,
                    self.TASK_CRASHED_TIMEOUT,
                    task.headers.get("receiver", "<unknown>"),
                )
            else:
                running_root_tasks.add(task.root_uid)

        if to_delete:
            to_increment = [
                task.headers.get("receiver", "unknown") for task in to_delete
            ]
            self.backend.delete_tasks(to_delete)
            self.backend.increment_metrics_list(
                KartonMetrics.TASK_GARBAGE_COLLECTED, to_increment
            )

        for finished_root_task in root_tasks.difference(running_root_tasks):
            # TODO: Notification needed
            self.log.debug("GC: Finished root task %s", finished_root_task)

    def gc_collect(self) -> None:
        if time.time() > (self.last_gc_trigger + self.gc_interval):
            try:
                self.gc_collect_abandoned_queues()
                self.gc_collect_tasks()
                self.gc_collect_resources()
            except Exception:
                self.log.exception("GC: Exception during garbage collection")
            self.last_gc_trigger = time.time()

    def route_task(self, task: Task, binds: List[KartonBind]) -> None:
        # Performs routing of task
        self.log.info("[%s] Processing task %s", task.root_uid, task.uid)
        # store the producer-task relationship in redis for task tracking
        self.backend.log_identity_output(
            task.headers.get("origin", "unknown"), task.headers
        )

        pipe = self.backend.make_pipeline()
        for bind in binds:
            identity = bind.identity
            if task.matches_filters(bind.filters):
                routed_task = task.fork_task()
                routed_task.status = TaskState.SPAWNED
                routed_task.last_update = time.time()
                routed_task.headers.update({"receiver": identity})
                self.backend.register_task(routed_task, pipe=pipe)
                self.backend.produce_routed_task(identity, routed_task, pipe=pipe)
                self.backend.increment_metrics(
                    KartonMetrics.TASK_ASSIGNED, identity, pipe=pipe
                )
        pipe.execute()

    def handle_tasks(self, task_uids: List[str]) -> None:
        tasks = self.backend.get_tasks(task_uids)
        binds = self.backend.get_binds()
        for task in tasks:
            if task is None:
                raise RuntimeError(
                    "Task disappeared while popping, this should never happen"
                )

            self.route_task(task, binds)
            task.last_update = time.time()
            task.status = TaskState.FINISHED
            # Directly update the unrouted task status to be finished
        self.backend.register_tasks(tasks)

    def handle_operations(self, bodies: List[str]) -> None:
        """
        Left for backwards compatibility with Karton <=4.3.0.
        Earlier versions delegate task status change to karton.system.
        """
        operation_bodies = []
        tasks = []
        for body in bodies:
            operation_body = json.loads(body)
            task = Task.unserialize(operation_body["task"])
            new_status = TaskState(operation_body["status"])
            if task.status != new_status:
                task.last_update = time.time()
                task.status = new_status
                tasks.append(task)
            operation_bodies.append(operation_body)

        self.backend.register_tasks(tasks)
        self.backend.produce_logs(
            operation_bodies, logger_name=KARTON_OPERATIONS_QUEUE, level="INFO"
        )

    def process_routing(self) -> None:
        # Order does matter! task dispatching must be before
        # karton.operations to avoid races. Timeout must be shorter
        # than self.gc_interval, but not too long allowing graceful shutdown
        data = self.backend.consume_queues(
            [KARTON_TASKS_QUEUE, KARTON_OPERATIONS_QUEUE], timeout=5
        )
        if data:
            queue, body = data
            if not isinstance(body, str):
                body = body.decode("utf-8")
            if queue == KARTON_TASKS_QUEUE:
                tasks = [body] + self.backend.consume_queues_batch(queue, max_count=100)
                self.handle_tasks(tasks)
            elif queue == KARTON_OPERATIONS_QUEUE:
                bodies = [body] + self.backend.consume_queues_batch(
                    queue, max_count=1000
                )
                self.handle_operations(bodies)

    def loop(self) -> None:
        self.log.info("Manager %s started", self.identity)

        while not self.shutdown:
            if self.enable_router:
                # This will wait for up to 5s, so this is not a busy loop
                self.process_routing()
            if self.enable_gc:
                # This will only do anything once every self.gc_interval seconds
                self.gc_collect()
                if not self.enable_router:
                    time.sleep(1)  # Avoid a busy loop

    @classmethod
    def args_parser(cls) -> argparse.ArgumentParser:
        parser = super().args_parser()
        parser.add_argument(
            "--setup-bucket", action="store_true", help="Create missing bucket in MinIO"
        )
        parser.add_argument(
            "--disable-gc", action="store_true", help="Do not run GC in this instance"
        )
        parser.add_argument(
            "--disable-router",
            action="store_true",
            help="Do not run task routing in this instance",
        )
        parser.add_argument(
            "--gc-interval",
            type=int,
            default=cls.GC_INTERVAL,
            help="Garbage collection interval",
        )
        return parser

    def ensure_bucket_exsits(self, create: bool) -> bool:
        bucket_name = self.backend.default_bucket_name
        bucket_exists = self.backend.check_bucket_exists(bucket_name, create=create)
        if not bucket_exists:
            if create:
                self.log.info("Bucket %s was missing. Created a new one.", bucket_name)
            else:
                self.log.error(
                    "Bucket %s is missing! If you're sure that name is correct and "
                    "you don't want to create it manually, use --setup-bucket option",
                    bucket_name,
                )
                return False
        return True

    @classmethod
    def main(cls) -> None:
        parser = cls.args_parser()
        args = parser.parse_args()

        config = Config(args.config_file)
        enable_gc = not args.disable_gc
        enable_router = not args.disable_router
        service = SystemService(config, enable_gc, enable_router, args.gc_interval)

        if not service.ensure_bucket_exsits(args.setup_bucket):
            # If bucket doesn't exist without --setup-bucket: quit
            return

        service.loop()
