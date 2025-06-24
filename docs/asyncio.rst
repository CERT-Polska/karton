Support for asyncio (experimental)
==================================

.. versionadded:: 5.8.0

Karton v5.8.0 implements experimental support for asyncio. The intended use-case is to support:

- "auto-scalable" Consumers that are waiting for external job to be done for most of the time (e.g. sandbox executors)
- Producers in asyncio-based projects

.. warning::

    ``karton.core.asyncio`` requires at least Python 3.11

How to use it?
--------------

The basic usage is almost the same as in sync version. If you want to write a consumer, just import needed things
from ``karton.core.asyncio`` package and use ``async def`` keyword in ``process(...)`` method.

.. code-block:: python

    import asyncio
    from karton.core.asyncio import Consumer, Task

    class FooBarConsumer(Consumer):
        identity = "foobar-consumer"
        filters = [
            {
                "type": "foobar"
            }
        ]

        async def process(self, task: Task) -> None:
            num = task.get_payload("data")
            self.log.info("Got number %d", num)
            await asyncio.sleep(5)
            if num % 3 == 0:
                self.log.info("Foo")
            if num % 5 == 0:
                self.log.info("Bar")

    if __name__ == "__main__":
        # calls asyncio.run(FooBarConsumer().loop())
        FooBarConsumer.main()


Using a Producer is similar, but you need to remember to call ``async connect()`` in the initialization code before sending a first task.
Synchronous version of KartonBackend connects to the Redis/S3 in the Producer constructor, but in asyncio, connection must be done explicitly.

.. code-block:: python

    import asyncio
    from karton.core.asyncio import Producer, Task

    foo_producer = Producer(identity="foobar-producer")

    async def main():
        await foo_producer.connect()

        for i in range(5):
            task = Task(headers={"type": "foobar"}, payload={"data": i})
            await foo_producer.send_task(task)

    if __name__ == "__main__":
        asyncio.run(main())

Limiting the Consumer concurrency
---------------------------------

asyncio Consumers are very greedy when it comes to consuming tasks. Each task is started as soon as possible and
proper `process()` coroutine is scheduled in event loop. It's recommended to set a limit of concurrently running
tasks via ``concurrency_limit`` configuration argument.

.. code-block:: python

    import asyncio
    from karton.core.asyncio import Consumer, Task

    class FooBarConsumer(Consumer):
        identity = "foobar-consumer"
        filters = [
            {
                "type": "foobar"
            }
        ]

        concurrency_limit = 16

Choosing the appropriate limit depends on how many of the parallel connections/jobs can be handled by the service
that is used by the Consumer.

Asynchronous resources
----------------------

Resources provided in Tasks are deserialized into ``karton.core.asyncio.RemoteResource`` objects.

There are few differences in their API compared to the synchronous version:

- all downloading methods need to be called with ``await`` keyword (they're coroutines).
- ``RemoteResource.content`` raises ``RuntimeError`` when resource wasn't explicitly downloaded before.
  You need to call ``await resource.download()`` first.

It's also required to use ``karton.core.asyncio.LocalResource`` while creating a new task.

Termination handling
--------------------

Asynchronous consumers must be aware of `task cancellation <https://docs.python.org/3/library/asyncio-task.html#task-cancellation>`_
and handle the `asyncio.CancelledError <https://docs.python.org/3/library/asyncio-exceptions.html#asyncio.CancelledError>`_
if they want to gracefully terminate their operations in case of ``SIGINT``/``SIGTERM`` or exceeded ``task_timeout``.

Asynchronous Karton can't interrupt blocking/hanged operations.

Known issues: reported number of replicas
-----------------------------------------

When using asyncio-based Karton consumers, be aware that the reported number of replicas may not accurately reflect
the actual number of running consumer instances.

This is due to how the Karton framework determines the replica count — it relies on counting active Redis connections.

Missing features
----------------

``karton.core.asyncio`` implements only a subset of Karton API, required to run most common producers/consumers.

Right now we don't support:

- test suite (``karton.core.test``)
- Karton state inspection (``karton.core.inspect``)
- pre/post/signalling hooks
