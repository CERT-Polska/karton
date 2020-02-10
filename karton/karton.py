"""
Base library for karton subsystems.
"""
import contextlib
import json
import time

from .base import KartonBase
from .resource import RemoteResource, RemoteDirectoryResource
from .task import Task, TaskState, TaskPriority
from .utils import GracefulKiller

TASKS_QUEUE = "karton.tasks"
TASK_PREFIX = "karton.task:"


class Producer(KartonBase):
    def send_task(self, task):
        """
        Sends a task to the RabbitMQ queue. Takes care of logging.
        Given task will be child of task we are currently handling (if such exists) - this ensures our log continuity

        :param task: task to be sent
        :type task: :py:class:`karton.Task`
        :rtype: bool
        :return: if task was delivered
        """
        task_id = str(task.uid)

        self.log.debug("Dispatched task {}".format(task_id))

        # Complete information about task
        if self.current_task is not None:
            task.set_task_parent(self.current_task)
            task.merge_persistent_payload(self.current_task)
            task.priority = self.current_task.priority

        task.last_update = time.time()
        task.headers.update({"origin": self.identity})

        # Ensure all local resources have good buckets
        for name, resource in task.get_resources():
            if (
                type(resource) is not RemoteResource
                and type(resource) is not RemoteDirectoryResource
            ):
                resource.bucket = self.config.minio_config["bucket"]

        task_json = task.serialize()

        # Declare task
        self.rs.set(TASK_PREFIX + task_id, task_json)

        # Upload local resources
        for payload_bag in [task.payload, task.payload_persistent]:
            for name, resource in payload_bag.resources():
                if (
                    type(resource) is not RemoteResource
                    and type(resource) is not RemoteDirectoryResource
                ):
                    payload_bag[name] = resource.upload(self.minio, resource.bucket)

        # Add task to TASKS_QUEUE
        self.rs.rpush(TASKS_QUEUE, task_id)
        return True

    @contextlib.contextmanager
    def continue_asynchronic(self, task, finish=True):
        """
        Continue asynchronic task. This is wrapper code used for resuming asynchronic task, takes care of setting
        context and logging when the task is finished. That is when the context manager finishes.

        :param task: task to be resumed, most likely Task({}, root_uid=root_uid, uid=uid)
        :type task: :py:class:`karton.Task`
        :param finish: if we should log that the task finished
        :type finish: bool
        """

        old_current_task = self.current_task

        self.current_task = task
        self.log_handler.set_task(self.current_task)

        # Handle task
        yield

        # Finish task
        if finish:
            self.declare_task_state(
                self.current_task, status=TaskState.FINISHED, identity=self.identity,
            )

        self.current_task = old_current_task
        self.log_handler.set_task(self.current_task)


class Consumer(KartonBase):
    """
    Base consumer class, expected to be inherited from

    Usage example:

        .. code-block:: python

            from karton import Consumer

            class Reporter(Consumer):
                identity = "karton.reporter"
                filters = [
                    {
                        "type": "sample",
                        "stage": "recognized"
                    },
                    {
                        "type": "config"
                    }
                ]
                def process():
                    # handle self.current_task, eg.
                    if self.current_task.headers["type"] == "sample":
                       return self.process_sample()
                    else:
                       return self.process_config()


    """

    filters = None

    def __init__(self, config):
        super(Consumer, self).__init__(config=config)

        self.registration = None
        self.current_task = None
        self.shutdown = False
        self.killer = GracefulKiller(self.graceful_shutdown)

    def graceful_shutdown(self):
        self.log.info("Gracefully shutting down!")
        self.shutdown = True

    def process(self):
        """
        Expected to be overwritten

        self.current_task contains task that triggered invocation of :py:meth:`karton.Consumer.process`
        """
        raise NotImplementedError()

    def internal_process(self, data):
        self.current_task = Task.unserialize(self.rs.get("karton.task:" + data))
        self.log_handler.set_task(self.current_task)

        for bind in self.filters:
            if self.current_task.matches_bind(bind):
                break
        else:
            self.log.info("Task rejected because binds are no longer valid.")
            self.declare_task_state(
                self.current_task, TaskState.FINISHED, identity=self.identity,
            )
            return

        try:
            self.log.info("Received new task - {}".format(self.current_task.uid))
            self.declare_task_state(
                self.current_task, TaskState.STARTED, identity=self.identity
            )
            self.process()
            self.log.info("Task done - {}".format(self.current_task.uid))
        except Exception:
            self.log.exception(
                "Failed to process task - {}".format(self.current_task.uid)
            )
        finally:
            if not self.current_task.is_asynchronic():
                self.declare_task_state(
                    self.current_task, TaskState.FINISHED, identity=self.identity,
                )

    def loop(self):
        """
        Blocking loop that consumes tasks and runs :py:meth:`karton.Consumer.process` as a handler
        """
        self.log.info("Service {} started".format(self.identity))

        self.registration = json.dumps(self.filters, sort_keys=True)
        old_registration = self.rs.hset(
            "karton.binds", self.identity, self.registration
        )

        if not old_registration:
            self.log.info("Service binds created.")
        elif old_registration != self.registration:
            self.log.info("Binds changed, old service instances should exit soon.")

        for task_filter in self.filters:
            self.log.info("Binding on: {}".format(task_filter))

        self.rs.client_setname(self.identity)

        while not self.shutdown:
            if self.rs.hget("karton.binds", self.identity) != self.registration:
                self.log.info("Binds changed, shutting down.")
                break

            item = self.rs.blpop(
                [
                    self.identity,  # Backwards compatibility, remove after upgrade
                    "karton.queue.{}:{}".format(TaskPriority.HIGH, self.identity),
                    "karton.queue.{}:{}".format(TaskPriority.NORMAL, self.identity),
                    "karton.queue.{}:{}".format(TaskPriority.LOW, self.identity),
                ],
                timeout=5,
            )

            if item:
                queue, data = item
                self.internal_process(data)

    def download_resource(self, resource):
        """
        Download remote resource into local resource.

        :param resource: resource to download
        :type resource: :py:class:`karton.RemoteResource`
        :rtype: :py:class:`karton.Resource`
        :return: downloaded resource
        """
        return resource.download(self.minio)

    @contextlib.contextmanager
    def download_to_temporary_folder(self, resource):
        """
        Context manager for downloading remote directory resource into local temporary folder.
        It also makes sure that the temporary folder is disposed afterwards.

        :param resource: resource to download
        :type resource: :py:class:`karton.RemoteDirectoryResource`
        :raises: TypeError
        :rtype: str
        :return: path to temporary folder with unpacked contents
        """
        if not resource.is_directory():
            raise TypeError(
                "Attempted to download resource that is NOT a directory as a directory."
            )
        with resource.download_to_temporary_folder(self.minio) as fpath:
            yield fpath

    def download_zip_file(self, resource):
        """
        Download remote directory resource contents into Zipfile object.

        :param resource: resource to download
        :type resource: :py:class:`karton.RemoteDirectoryResource`
        :rtype: :py:class:`zipfile.Zipfile`
        :return: zipfile with downloaded contents
        """
        if not resource.is_directory():
            raise TypeError(
                "Attempted to download resource that is NOT a directory as a directory."
            )
        return resource.download_zip_file(self.minio)


class Karton(Consumer, Producer):
    """
    This glues together Consumer and Producer - which is the most common use case
    """
