import time
from typing import Optional

from karton.core import Config, Task
from karton.core.resource import LocalResource as SyncLocalResource

from ..backend import KartonMetrics
from .backend import KartonAsyncBackend
from .base import KartonAsyncBase
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
