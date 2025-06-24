import abc
import argparse
import asyncio
import sys
import time
import traceback
from asyncio import CancelledError
from typing import Any, Dict, List, Optional

from karton.core import query
from karton.core.__version__ import __version__
from karton.core.backend import KartonBind, KartonMetrics
from karton.core.config import Config
from karton.core.exceptions import TaskTimeoutError
from karton.core.resource import LocalResource as SyncLocalResource
from karton.core.task import Task, TaskState

from .backend import KartonAsyncBackend
from .base import KartonAsyncBase, KartonAsyncServiceBase
from .resource import LocalResource


class Producer(KartonAsyncBase):
    """
    Producer part of Karton. Used for dispatching initial tasks into karton.

    :param config: Karton configuration object (optional)
    :type config: :class:`karton.Config`
    :param identity: Producer name (optional)
    :type identity: str

    Usage example:

    .. code-block:: python

        from karton.core.asyncio import Producer

        producer = Producer(identity="karton.mwdb")
        await producer.connect()
        task = Task(
            headers={
                "type": "sample",
                "kind": "raw"
            },
            payload={
                "sample": Resource("sample.exe", b"put content here")
            }
        )
        await producer.send_task(task)

    :param config: Karton config to use for service configuration
    :param identity: Karton producer identity
    :param backend: Karton backend to use
    """

    def __init__(
        self,
        config: Optional[Config] = None,
        identity: Optional[str] = None,
        backend: Optional[KartonAsyncBackend] = None,
    ) -> None:
        super().__init__(config=config, identity=identity, backend=backend)

    async def send_task(self, task: Task) -> bool:
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
            if isinstance(resource, SyncLocalResource):
                raise RuntimeError(
                    "Synchronous resources are not supported. "
                    "Use karton.core.asyncio.resource module instead."
                )

        # Register new task
        await self.backend.register_task(task)

        # Upload local resources
        for resource in task.iterate_resources():
            if isinstance(resource, LocalResource):
                await resource.upload(self.backend)

        # Add task to karton.tasks
        await self.backend.produce_unrouted_task(task)
        await self.backend.increment_metrics(KartonMetrics.TASK_PRODUCED, self.identity)
        return True


class Consumer(KartonAsyncServiceBase):
    """
    Base consumer class, this is the part of Karton responsible for processing
    incoming tasks

    :param config: Karton config to use for service configuration
    :param identity: Karton service identity
    :param backend: Karton backend to use
    :param task_timeout: The maximum time, in seconds, this consumer will wait for
                         a task to finish processing before being CRASHED on timeout.
                         Set 0 for unlimited, and None for using global value
    :param concurrency_limit: The maximum number of concurrent tasks that may be
                        gathered from queue and processed asynchronously.
    """

    filters: List[Dict[str, Any]] = []
    persistent: bool = True
    version: Optional[str] = None
    task_timeout = None
    concurrency_limit: Optional[int] = 1

    def __init__(
        self,
        config: Optional[Config] = None,
        identity: Optional[str] = None,
        backend: Optional[KartonAsyncBackend] = None,
    ) -> None:
        super().__init__(config=config, identity=identity, backend=backend)

        if self.filters is None:
            raise ValueError("Cannot bind consumer on Empty binds")

        # Dummy conversion to make sure the filters are well-formed.
        query.convert(self.filters)

        self.persistent = (
            self.config.getboolean("karton", "persistent", self.persistent)
            and not self.debug
        )
        if self.task_timeout is None:
            self.task_timeout = self.config.getint("karton", "task_timeout")

        self.concurrency_limit = self.config.getint(
            "karton", "concurrency_limit", self.concurrency_limit
        )

        self.concurrency_semaphore: Optional[asyncio.Semaphore] = None
        if self.concurrency_limit is not None:
            self.concurrency_semaphore = asyncio.BoundedSemaphore(
                self.concurrency_limit
            )

    @abc.abstractmethod
    async def process(self, task: Task) -> None:
        """
        Task processing method.

        :param task: The incoming task object

        self.current_task contains task that triggered invocation of
        :py:meth:`karton.Consumer.process` but you should only focus on the passed
        task object and shouldn't interact with the field directly.
        """
        raise NotImplementedError()

    async def _internal_process(self, task: Task) -> None:
        exception_str = None
        try:
            self.log.info("Received new task - %s", task.uid)
            await self.backend.set_task_status(task, TaskState.STARTED)

            if self.task_timeout:
                try:
                    # asyncio.timeout is Py3.11+
                    async with asyncio.timeout(self.task_timeout):  # type: ignore
                        await self.process(task)
                except asyncio.TimeoutError as e:
                    raise TaskTimeoutError from e
            else:
                await self.process(task)
            self.log.info("Task done - %s", task.uid)
        except (Exception, TaskTimeoutError, CancelledError):
            exc_info = sys.exc_info()
            exception_str = traceback.format_exception(*exc_info)

            await self.backend.increment_metrics(
                KartonMetrics.TASK_CRASHED, self.identity
            )
            self.log.exception("Failed to process task - %s", task.uid)
        finally:
            await self.backend.increment_metrics(
                KartonMetrics.TASK_CONSUMED, self.identity
            )

            task_state = TaskState.FINISHED

            # report the task status as crashed
            # if an exception was caught while processing
            if exception_str is not None:
                task_state = TaskState.CRASHED
                task.error = exception_str

            await self.backend.set_task_status(task, task_state)

    async def internal_process(self, task: Task) -> None:
        """
        The internal side of :py:meth:`Consumer.process` function, takes care of
        synchronizing the task state, handling errors and running task hooks.

        :param task: Task object to process

        :meta private:
        """
        try:
            self.current_task = task

            if not task.matches_filters(self.filters):
                self.log.info("Task rejected because binds are no longer valid.")
                await self.backend.set_task_status(task, TaskState.FINISHED)
                # Task rejected: end of processing
                return

            await self._internal_process(task)
        finally:
            if self.concurrency_semaphore is not None:
                self.concurrency_semaphore.release()
            self.current_task = None

    @property
    def _bind(self) -> KartonBind:
        return KartonBind(
            identity=self.identity,
            info=self.__class__.__doc__,
            version=__version__,
            filters=self.filters,
            persistent=self.persistent,
            service_version=self.__class__.version,
            is_async=True,
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
        parser.add_argument(
            "--concurrency-limit",
            type=int,
            help="Limit number of concurrent tasks",
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
                    "concurrency_limit": args.concurrency_limit,
                }
            }
        )

    async def _loop(self) -> None:
        """
        Blocking loop that consumes tasks and runs
        :py:meth:`karton.Consumer.process` as a handler

        :meta private:
        """
        self.log.info("Service %s started", self.identity)

        if self.task_timeout:
            self.log.info(f"Task timeout is set to {self.task_timeout} seconds")
        if self.concurrency_limit:
            self.log.info(f"Concurrency limit is set to {self.concurrency_limit}")

        # Get the old binds and set the new ones atomically
        old_bind = await self.backend.register_bind(self._bind)

        if not old_bind:
            self.log.info("Service binds created.")
        elif old_bind != self._bind:
            self.log.info("Binds changed, old service instances should exit soon.")

        for task_filter in self.filters:
            self.log.info("Binding on: %s", task_filter)

        concurrent_tasks: List[asyncio.Task] = []

        try:
            while True:
                current_bind = await self.backend.get_bind(self.identity)
                if current_bind != self._bind:
                    self.log.info("Binds changed, shutting down.")
                    break
                if self.concurrency_semaphore is not None:
                    await self.concurrency_semaphore.acquire()
                task = await self.backend.consume_routed_task(self.identity)
                if task:
                    coro_task = asyncio.create_task(self.internal_process(task))
                    concurrent_tasks.append(coro_task)
                else:
                    if self.concurrency_semaphore is not None:
                        self.concurrency_semaphore.release()
                # Garbage collection and exception propagation
                # for finished concurrent tasks
                unfinished_tasks: List[asyncio.Task] = []
                for coro_task in concurrent_tasks:
                    if coro_task.done():
                        # Propagate possible unhandled exception
                        coro_task.result()
                    else:
                        unfinished_tasks.append(coro_task)
                concurrent_tasks = unfinished_tasks
        finally:
            # Finally handles shutdown events:
            # - main loop cancellation (SIGINT/SIGTERM)
            # - unhandled exception in internal_process
            # First cancel all pending tasks
            for coro_task in concurrent_tasks:
                if not coro_task.done():
                    coro_task.cancel()
            # Then gather all tasks to finalize them
            await asyncio.gather(*concurrent_tasks)


class Karton(Consumer, Producer):
    """
    This glues together Consumer and Producer - which is the most common use case
    """

    def __init__(
        self,
        config: Optional[Config] = None,
        identity: Optional[str] = None,
        backend: Optional[KartonAsyncBackend] = None,
    ) -> None:
        super().__init__(config=config, identity=identity, backend=backend)
