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

    def __init__(self, config: Optional[Config]) -> None:
        super(SystemService, self).__init__(config=config)
        self.last_gc_trigger = 0.0

    def gc_collect_resources(self) -> None:
        karton_bucket = self.backend.default_bucket_name
        resources_to_remove = set(self.backend.list_objects(karton_bucket))
        tasks = self.backend.get_all_tasks()
        for task in tasks:
            for _, resource in task.iterate_resources():
                # If resource is referenced by task: remove it from set
                if (
                    resource.bucket == karton_bucket
                    and resource.uid in resources_to_remove
                ):
                    resources_to_remove.remove(resource.uid)
        for object_name in list(resources_to_remove):
            try:
                self.backend.remove_object(karton_bucket, object_name)
                self.log.debug(
                    "GC: Removed unreferenced resource %s:%s",
                    karton_bucket,
                    object_name,
                )
            except Exception:
                self.log.exception(
                    "GC: Error during resource removing %s:%s",
                    karton_bucket,
                    object_name,
                )

    def gc_collect_tasks(self) -> None:
        root_tasks = set()
        running_root_tasks = set()
        tasks = self.backend.get_all_tasks()
        enqueued_task_uids = self.backend.get_task_ids_from_queue(KARTON_TASKS_QUEUE)
        current_time = time.time()
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
                self.backend.delete_task(task)
                self.backend.increment_metrics(
                    KartonMetrics.TASK_GARBAGE_COLLECTED,
                    task.headers.get("receiver", "unknown"),
                )
            else:
                running_root_tasks.add(task.root_uid)
        for finished_root_task in root_tasks.difference(running_root_tasks):
            # TODO: Notification needed
            self.log.debug("GC: Finished root task %s", finished_root_task)

    def gc_collect(self) -> None:
        if time.time() > (self.last_gc_trigger + self.GC_INTERVAL):
            try:
                self.gc_collect_tasks()
                self.gc_collect_resources()
            except Exception:
                self.log.exception("GC: Exception during garbage collection")
            self.last_gc_trigger = time.time()

    def process_task(self, task: Task) -> None:
        online_consumers = self.backend.get_online_consumers()

        self.log.info("[%s] Processing task %s", task.root_uid, task.uid)

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
            # Order does matter! task dispatching must be before
            # karton.operations to avoid races Timeout must be shorter than GC_INTERVAL,
            # but not too long allowing graceful shutdown
            data = self.backend.consume_queues(
                ["karton.tasks", "karton.operations"], timeout=5
            )
            if data:
                queue, body = data
                if not isinstance(body, str):
                    body = body.decode("utf-8")
                if queue == "karton.tasks":
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
                elif queue == "karton.operations":
                    operation_body = json.loads(body)
                    task = Task.unserialize(operation_body["task"])
                    new_status = TaskState(operation_body["status"])
                    if task.status != new_status:
                        task.last_update = time.time()
                        task.status = new_status
                        self.log.info(
                            "[%s] %s %s task %s",
                            str(task.root_uid),
                            operation_body["identity"],
                            operation_body["status"],
                            str(task.uid),
                        )
                        # Update task status
                        self.backend.register_task(task)
                    # Pass new operation status to log
                    self.backend.produce_log(
                        operation_body, logger_name="karton.operations", level="INFO"
                    )
            self.gc_collect()

    @classmethod
    def args_parser(cls) -> argparse.ArgumentParser:
        parser = super().args_parser()
        parser.add_argument(
            "--setup-bucket", action="store_true", help="Create missing bucket in MinIO"
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
        service = SystemService(config)

        if not service.ensure_bucket_exsits(args.setup_bucket):
            # If bucket doesn't exist without --setup-bucket: quit
            return

        service.loop()
