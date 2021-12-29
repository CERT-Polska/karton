import argparse
import json
import time
from typing import Optional

from karton.core.__version__ import __version__
from karton.core.backend import KARTON_TASKS_QUEUE, KartonMetrics
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
        gc_inteval: int = 3*60,
    ) -> None:
        super(SystemService, self).__init__(config=config)
        self.last_gc_trigger = time.time()
        self.enable_gc = enable_gc
        self.enable_router = enable_router
        self.gc_inteval = gc_inteval
        self.totaltasks = 0
        self.totalops = 0

    def gc_collect_resources(self) -> None:
        karton_bucket = self.backend.default_bucket_name
        start = time.time()
        resources_to_remove = set(self.backend.list_objects(karton_bucket))
        self.log.info("Resources: List objects: %s", time.time() - start)
        self.log.info("Resources: List objects: %s objects", len(resources_to_remove))
        start = time.time()
        tasks = self.backend.get_all_tasks()
        self.log.info("Resources: Get all tasks: %s", time.time() - start)
        start = time.time()
        for task in tasks:
            for _, resource in task.iterate_resources():
                # If resource is referenced by task: remove it from set
                if (
                    resource.bucket == karton_bucket
                    and resource.uid in resources_to_remove
                ):
                    resources_to_remove.remove(resource.uid)
        self.log.info("Resources: For loop: %s", time.time() - start)
        start = time.time()
        if resources_to_remove:
            self.log.info("Resources: Removing %s resources", len(resources_to_remove))
            self.backend.remove_objects(karton_bucket, list(resources_to_remove))
        self.log.info("Resources: Remove loop: %s", time.time() - start)

    def gc_collect_tasks(self) -> None:
        root_tasks = set()
        running_root_tasks = set()
        start = time.time()
        tasks = self.backend.get_all_tasks()
        self.log.info("Tasks: Get all tasks: %s", time.time() - start)
        enqueued_task_uids = self.backend.get_task_ids_from_queue(KARTON_TASKS_QUEUE)

        current_time = time.time()
        to_delete = []
        for task in tasks:
            root_tasks.add(task.root_uid)
            will_delete = False
            if (
                task.status == TaskState.DECLARED
                and task.uid not in enqueued_task_uids
                and task.last_update is not None
                and current_time > task.last_update + self.TASK_DISPATCHED_TIMEOUT
            ):
                will_delete = True
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
                will_delete = True
                self.log.warning(
                    "Task %s is in Started state more than %d seconds. "
                    "Killed. (receiver: %s)",
                    task.uid,
                    self.TASK_STARTED_TIMEOUT,
                    task.headers.get("receiver", "<unknown>"),
                )
            elif task.status == TaskState.FINISHED:
                will_delete = True
                self.log.debug("GC: Finished task %s", task.uid)
            elif (
                task.status == TaskState.CRASHED
                and task.last_update is not None
                and current_time > task.last_update + self.TASK_CRASHED_TIMEOUT
            ):
                will_delete = True
                self.log.debug(
                    "GC: Task %s is in Crashed state more than %d seconds. "
                    "Killed. (receiver: %s)",
                    task.uid,
                    self.TASK_CRASHED_TIMEOUT,
                    task.headers.get("receiver", "<unknown>"),
                )
            if will_delete:
                to_delete.append(task)
            else:
                running_root_tasks.add(task.root_uid)

        start = time.time()
        if to_delete:
            self.log.info("Tasks: Deleting %s tasks", len(to_delete))
            to_increment = [task.headers.get("receiver", "unknown") for tas in to_delete]
            self.backend.delete_tasks(to_delete)
            self.backend.increment_metrics_list(KartonMetrics.TASK_GARBAGE_COLLECTED, to_increment)
        self.log.info("Tasks: Big loop: %s", time.time() - start)

        for finished_root_task in root_tasks.difference(running_root_tasks):
            # TODO: Notification needed
            self.log.debug("GC: Finished root task %s", finished_root_task)

    def handle_tasks(self, bodies) -> None:
        for body in bodies:
            task_uid = body
            task = self.backend.get_task(task_uid)
            if task is None:
                raise RuntimeError(
                    "Task disappeared while popping, this should never happen"
                )

            self.process_task(task)
            task.last_update = time.time()
            task.status = TaskState.FINISHED
            # Directly update the task status to be finished
            self.backend.register_task(task)

    def handle_operations(self, bodies) -> None:
        self.log.info("Handling a batch of %s operatoins", len(bodies))
        operation_bodies = []
        tasks = []
        for body in bodies:
            operation_body = json.loads(body)
            task = Task.unserialize(operation_body["task"])
            new_status = TaskState(operation_body["status"])
            if task.status != new_status:
                task.last_update = time.time()
                task.status = new_status
                # self.log.info(
                #     "[%s] %s %s task %s",
                #     str(task.root_uid),
                #     operation_body["identity"],
                #     operation_body["status"],
                #     str(task.uid),
                # )
                tasks.append(task)
            operation_bodies.append(operation_body)

        start = time.time()
        self.backend.register_tasks(tasks)
        self.log.info("Registering: %s", time.time() - start)

        start = time.time()
        # for operation_body in operation_bodies:
            # Pass new operation status to log
        self.backend.produce_logs(
            operation_bodies, logger_name="karton.operations", level="INFO"
        )
        self.log.info("Logging: %s", time.time() - start)



    def process_routing(self) -> None:
        # Order does matter! task dispatching must be before
        # karton.operations to avoid races Timeout must be shorter than self.gc_inteval,
        # but not too long allowing graceful shutdown
        data = self.backend.consume_queues(
            ["karton.tasks", "karton.operations"], timeout=5
        )
        if data:
            queue, body = data
            start = time.time()
            if not isinstance(body, str):
                body = body.decode("utf-8")
            if queue == "karton.tasks":
                tasks = [body] + self.backend.consume_queues_batch(queue, 100)
                self.handle_tasks(tasks)
                self.totaltasks += time.time() - start
            elif queue == "karton.operations":
                bodies = [body] + self.backend.consume_queues_batch(queue, 1000)
                self.handle_operations(bodies)
                self.totalops += time.time() - start

            self.log.info("Tasks: %s Ops: %s XY: %s", self.totaltasks, self.totalops, self.totaltasks / (self.totalops + 0.0001))

    def gc_collect(self) -> None:
        if time.time() > (self.last_gc_trigger + self.gc_inteval):
            try:
                self.gc_collect_tasks()
                self.gc_collect_resources()
            except Exception:
                self.log.exception("GC: Exception during garbage collection")
            self.log.info("GC done in %s seconds", time.time() - self.last_gc_trigger)
            self.last_gc_trigger = time.time()

    def process_task(self, task: Task) -> None:
        online_consumers = self.backend.get_online_consumers()

        # self.log.info("[%s] Processing task %s", task.root_uid, task.uid)

        for bind in self.backend.get_binds():
            identity = bind.identity
            if identity not in online_consumers and not bind.persistent:
                # If unbound and not persistent
                for queue in self.backend.get_queue_names(identity):
                    self.log.info(
                        "Non-persistent: unwinding tasks from queue %s", queue
                    )
                    removed_tasks = self.backend.remove_task_queue(queue)
                    for removed_task in removed_tasks:
                        self.log.info("Unwinding task %s", str(removed_task.uid))
                        # Let the karton.system loop finish this task
                        self.backend.set_task_status(
                            removed_task, TaskState.FINISHED, consumer=identity
                        )
                self.log.info("Non-persistent: removing bind %s", identity)
                self.backend.unregister_bind(identity)
                # Since this bind was deleted we can skip the task bind matching
                continue

            if task.matches_filters(bind.filters):
                routed_task = task.fork_task()
                routed_task.status = TaskState.SPAWNED
                routed_task.last_update = time.time()
                routed_task.headers.update({"receiver": identity})
                self.backend.register_task(routed_task)
                self.backend.produce_routed_task(identity, routed_task)
                self.backend.set_task_status(
                    routed_task, TaskState.SPAWNED, consumer=identity
                )
                self.backend.increment_metrics(KartonMetrics.TASK_ASSIGNED, identity)

    def loop(self) -> None:
        self.log.info("Manager {} started".format(self.identity))

        while not self.shutdown:
            if self.enable_router:
                # This will wait for up to 5s, so this is not a busy loop
                self.process_routing()
            if self.enable_gc:
                # This will only do anything once every self.gc_inteval seconds
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
            "--disable-router", action="store_true", help="Create missing bucket in MinIO"
        )
        parser.add_argument(
            "--gc-interval", type=int, default=3*60, help="Garbage collection interval"
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
        gc_interval = not args.disable_router
        service = SystemService(config, enable_gc, enable_router, args.gc_interval)

        if not service.ensure_bucket_exsits(args.setup_bucket):
            # If bucket doesn't exist without --setup-bucket: quit
            return

        service.loop()
