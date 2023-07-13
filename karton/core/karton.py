"""
Base library for karton subsystems.
"""
import abc
import argparse
import sys
import time
import traceback
from typing import Any, Callable, Dict, List, Optional, Tuple, cast

from .__version__ import __version__
from .backend import KartonBackend, KartonBind, KartonMetrics
from .base import KartonBase, KartonServiceBase
from .config import Config
from .resource import LocalResource
from .task import Task, TaskState
from .utils import timeout


class Producer(KartonBase):
    """
    Producer part of Karton. Used for dispatching initial tasks into karton.

    :param config: Karton configuration object (optional)
    :type config: :class:`karton.Config`
    :param identity: Producer name (optional)
    :type identity: str

    Usage example:

    .. code-block:: python

        from karton.core import Producer

        producer = Producer(identity="karton.mwdb")
        task = Task(
            headers={
                "type": "sample",
                "kind": "raw"
            },
            payload={
                "sample": Resource("sample.exe", b"put content here")
            }
        )
        producer.send_task(task)

    :param config: Karton config to use for service configuration
    :param identity: Karton producer identity
    :param backend: Karton backend to use
    """

    def __init__(
        self,
        config: Optional[Config] = None,
        identity: Optional[str] = None,
        backend: Optional[KartonBackend] = None,
    ) -> None:
        super().__init__(config=config, identity=identity, backend=backend)

    def send_task(self, task: Task) -> bool:
        """
        Sends a task to the unrouted task queue. Takes care of logging.
        Given task will be child of task we are currently handling (if such exists).

        :param task: Task object to be sent
        :return: Bool indicating if the task was delivered
        """
        self.log.debug("Dispatched task %s", task.uid)

        # Complete information about task
        if self.current_task is not None:
            task.set_task_parent(self.current_task)
            task.merge_persistent_payload(self.current_task)
            task.merge_persistent_headers(self.current_task)
            task.priority = self.current_task.priority

        task.last_update = time.time()
        task.headers.update({"origin": self.identity})

        # Ensure all local resources have good buckets
        for resource in task.iterate_resources():
            if isinstance(resource, LocalResource) and not resource.bucket:
                resource.bucket = self.backend.default_bucket_name

        # Register new task
        self.backend.register_task(task)

        # Upload local resources
        for resource in task.iterate_resources():
            if isinstance(resource, LocalResource):
                resource.upload(self.backend)

        # Add task to karton.tasks
        self.backend.produce_unrouted_task(task)
        self.backend.increment_metrics(KartonMetrics.TASK_PRODUCED, self.identity)
        return True


class Consumer(KartonServiceBase):
    """
    Base consumer class, this is the part of Karton responsible for processing
    incoming tasks

    :param config: Karton config to use for service configuration
    :param identity: Karton service identity
    :param backend: Karton backend to use
    """

    filters: List[Dict[str, Any]] = []
    persistent: bool = True
    version: Optional[str] = None

    def __init__(
        self,
        config: Optional[Config] = None,
        identity: Optional[str] = None,
        backend: Optional[KartonBackend] = None,
    ) -> None:
        super().__init__(config=config, identity=identity, backend=backend)

        if self.filters is None:
            raise ValueError("Cannot bind consumer on Empty binds")

        self.persistent = (
            self.config.getboolean("karton", "persistent", self.persistent)
            and not self.debug
        )
        self.task_timeout = self.config.getint("karton", "task_timeout")
        self.current_task: Optional[Task] = None
        self._pre_hooks: List[Tuple[Optional[str], Callable[[Task], None]]] = []
        self._post_hooks: List[
            Tuple[Optional[str], Callable[[Task, Optional[Exception]], None]]
        ] = []

    @abc.abstractmethod
    def process(self, task: Task) -> None:
        """
        Task processing method.

        :param task: The incoming task object

        self.current_task contains task that triggered invocation of
        :py:meth:`karton.Consumer.process` but you should only focus on the passed
        task object and shouldn't interact with the field directly.
        """
        raise NotImplementedError()

    def internal_process(self, task: Task) -> None:
        """
        The internal side of :py:meth:`Consumer.process` function, takes care of
        synchronizing the task state, handling errors and running task hooks.

        :param task: Task object to process

        :meta private:
        """

        self.current_task = task
        self.log_handler.set_task(self.current_task)

        if not self.current_task.matches_filters(self.filters):
            self.log.info("Task rejected because binds are no longer valid.")
            self.backend.set_task_status(self.current_task, TaskState.FINISHED)
            # Task rejected: end of processing
            return

        exception_str = None

        try:
            self.log.info("Received new task - %s", self.current_task.uid)
            self.backend.set_task_status(self.current_task, TaskState.STARTED)

            self._run_pre_hooks()

            saved_exception = None
            try:
                if self.task_timeout:
                    with timeout(self.task_timeout):
                        self.process(self.current_task)
                else:
                    self.process(self.current_task)
            except Exception as exc:
                saved_exception = exc
                raise
            finally:
                self._run_post_hooks(saved_exception)

            self.log.info("Task done - %s", self.current_task.uid)
        except Exception:
            exc_info = sys.exc_info()
            exception_str = traceback.format_exception(*exc_info)

            self.backend.increment_metrics(KartonMetrics.TASK_CRASHED, self.identity)
            self.log.exception("Failed to process task - %s", self.current_task.uid)
        finally:
            self.backend.increment_metrics(KartonMetrics.TASK_CONSUMED, self.identity)

            task_state = TaskState.FINISHED

            # report the task status as crashed
            # if an exception was caught while processing
            if exception_str is not None:
                task_state = TaskState.CRASHED
                self.current_task.error = exception_str

            self.backend.set_task_status(self.current_task, task_state)

    @property
    def _bind(self) -> KartonBind:
        return KartonBind(
            identity=self.identity,
            info=self.__class__.__doc__,
            version=__version__,
            filters=self.filters,
            persistent=self.persistent,
            service_version=self.__class__.version,
        )

    @classmethod
    def args_parser(cls) -> argparse.ArgumentParser:
        parser = super().args_parser()
        # store_false defaults to True, we intentionally want None there
        parser.add_argument(
            "--non-persistent",
            action="store_const",
            const=False,
            dest="persistent",
            help="Run service with non-persistent queue",
        )
        parser.add_argument(
            "--task-timeout",
            type=int,
            help="Limit task execution time",
        )
        return parser

    @classmethod
    def config_from_args(cls, config: Config, args: argparse.Namespace) -> None:
        super().config_from_args(config, args)
        config.load_from_dict(
            {
                "karton": {
                    "persistent": args.persistent,
                    "task_timeout": args.task_timeout,
                }
            }
        )

    def add_pre_hook(
        self, callback: Callable[[Task], None], name: Optional[str] = None
    ) -> None:
        """
        Add a function to be called before processing each task.

        :param callback: Function of the form ``callback(task)`` where ``task``
            is a :class:`karton.Task`
        :param name: Name of the pre-hook
        """
        self._pre_hooks.append((name, callback))

    def add_post_hook(
        self,
        callback: Callable[[Task, Optional[Exception]], None],
        name: Optional[str] = None,
    ) -> None:
        """
        Add a function to be called after processing each task.

        :param callback: Function of the form ``callback(task, exception)``
            where ``task`` is a :class:`karton.Task` and ``exception`` is
            an exception thrown by the :meth:`karton.Consumer.process` function
            or ``None``.
        :param name: Name of the post-hook
        """
        self._post_hooks.append((name, callback))

    def _run_pre_hooks(self) -> None:
        """
        Run registered preprocessing hooks

        :meta private:
        """
        for name, callback in self._pre_hooks:
            try:
                callback(cast(Task, self.current_task))
            except Exception:
                if name:
                    self.log.exception("Pre-hook (%s) failed", name)
                else:
                    self.log.exception("Pre-hook failed")

    def _run_post_hooks(self, exception: Optional[Exception]) -> None:
        """
        Run registered postprocessing hooks

        :param exception: Exception object that was caught while processing the task

        :meta private:
        """
        for name, callback in self._post_hooks:
            try:
                callback(cast(Task, self.current_task), exception)
            except Exception:
                if name:
                    self.log.exception("Post-hook (%s) failed", name)
                else:
                    self.log.exception("Post-hook failed")

    def loop(self) -> None:
        """
        Blocking loop that consumes tasks and runs
        :py:meth:`karton.Consumer.process` as a handler

        :meta private:
        """
        self.log.info("Service %s started", self.identity)

        if self.task_timeout is not None:
            self.log.info(f"Task timeout is set to {self.task_timeout} seconds")

        # Get the old binds and set the new ones atomically
        old_bind = self.backend.register_bind(self._bind)

        if not old_bind:
            self.log.info("Service binds created.")
        elif old_bind != self._bind:
            self.log.info("Binds changed, old service instances should exit soon.")

        for task_filter in self.filters:
            self.log.info("Binding on: %s", task_filter)

        with self.graceful_killer():
            while not self.shutdown:
                if self.backend.get_bind(self.identity) != self._bind:
                    self.log.info("Binds changed, shutting down.")
                    break
                task = self.backend.consume_routed_task(self.identity)
                if task:
                    self.internal_process(task)


class LogConsumer(KartonServiceBase):
    """
    Base class for log consumer subsystems.

    You can consume logs from specific logger
    by setting a :py:meth:`logger_filter` class attribute.

    You can also select logs of specific level via
    :py:meth:`level` class attribute.

    :param config: Karton config to use for service configuration
    :param identity: Karton service identity
    :param backend: Karton backend to use
    """

    logger_filter: Optional[str] = None
    level: Optional[str] = None
    with_service_info = True

    def __init__(
        self,
        config: Optional[Config] = None,
        identity: Optional[str] = None,
        backend: Optional[KartonBackend] = None,
    ) -> None:
        super().__init__(config=config, identity=identity, backend=backend)

    @abc.abstractmethod
    def process_log(self, event: Dict[str, Any]) -> None:
        """
        The core log handler that should be overwritten in implemented log handlers

        :param event: Dictionary containing the log event data
        """
        raise NotImplementedError()

    def loop(self) -> None:
        """
        Internal loop that consumes the log queues and deals with exceptions
        and graceful exits

        :meta private:
        """
        self.log.info("Logger %s started", self.identity)

        with self.graceful_killer():
            for log in self.backend.consume_log(
                logger_filter=self.logger_filter, level=self.level
            ):
                if self.shutdown:
                    # Consumer shutdown has been requested
                    break
                if not log:
                    # No log record received until timeout, try again.
                    continue
                try:
                    self.process_log(log)
                except Exception:
                    """
                    This is log handler exception, so DO NOT USE self.log HERE!
                    """
                    import traceback

                    traceback.print_exc()


class Karton(Consumer, Producer):
    """
    This glues together Consumer and Producer - which is the most common use case
    """

    def __init__(
        self,
        config: Optional[Config] = None,
        identity: Optional[str] = None,
        backend: Optional[KartonBackend] = None,
    ) -> None:
        super().__init__(config=config, identity=identity, backend=backend)

        if self.config.getboolean("signaling", "status", fallback=False):
            self.log.info("Using status signaling")
            self.add_pre_hook(self._send_signaling_status_task_begin, "task_begin")
            self.add_post_hook(self._send_signaling_status_task_end, "task_end")

    def _send_signaling_status_task_begin(self, task: Task) -> None:
        """Send a begin status signaling task.

        :meta private:
        """
        self._send_signaling_status_task("task_begin")

    def _send_signaling_status_task_end(
        self, task: Task, ex: Optional[Exception]
    ) -> None:
        """Send a begin status signaling task.

        :meta private:
        """
        self._send_signaling_status_task("task_end")

    def _send_signaling_status_task(self, status: str) -> None:
        """Send a status signaling task.

        :param status: Status task identifier.

        :meta private:
        """
        task = Task({"type": "karton.signaling.status", "status": status})
        self.send_task(task)
