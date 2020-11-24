Advanced concepts
=================

Task tree (analysis) and task life cycle
----------------------------------------

Every analysis starts from `initial task` spawned by :class:`karton.Producer`. `Initial task` is consumed by consumers, which then are producing next tasks for further processing. These various tasks originating from initial task can be grouped together into **task tree**, representing the analysis.

<image>

Each task is identified by a tuple of three identifiers:

- **uid** - unique task identifier
- **parent_uid** - identifier of task that spawned current task as a result of processing
- **root_uid** - task tree identifier (analysis identifier, derived from uid of initial **unrouted** task)

Routed and unrouted tasks (task forking)
````````````````````````````````````````

Brand-new :class:`karton.Task` instance is initialized with unique identifier. After call of :py:meth:`karton.Producer.send_task` method, it starts its life in **unrouted task queue**.

.. image:: forking-task.svg

Because task headers can be accepted by more than one consumer - task need to be forked before it goes to the appropriate **consumer queues**. Based on **unrouted task**, **Karton-system** generates as many **routed tasks** as there are matching queues. These tasks are separate, independent instances, so they have different **uid** than original unrouted task.

.. note::
    
    While **uid** of routed and unrouted tasks are different, **parent_uid** stays the same. **parent_uid** always identifies the routed task.

    Currently we don't keep the relationship between routed and unrouted task identifier, so there could be additional difficulty in task flow tracking when Kartonik spawns more than one task (**parent_uid** will be the same for all of them)


Task life cycle
```````````````
When :py:meth:`karton.Producer.send_task` is called: **unrouted task** starts its journey in `Declared` state. Task is registered in Redis with references to the Resource objects. After task declaration, :class:`karton.LocalResource` objects are uploaded to MinIO. Finally, task identifier is placed in **unrouted tasks queue** and is waiting for **Karton-System** to route it to appropriate consumers.

<image>

Task is fetched by **Karton-System** broker, which checks all the consumer queue filters if any of them match task headers. If there is a match, task is **forked** to the appropriate queue.

<image>

**Routed tasks** go to the `Spawned` state and their identifiers are placed in appropriate consumer queues (one copy of task per queue). Original unrouted task go to the `Finished` state.

Then, routed task is fetched by consumer and goes to the `Started` state. In this state, Karton subsystem performs the job and new tasks can be spawned as a result of processing. When job is finished: task goes to the `Finished` state and all information about task and unreferenced resources are removed by **Karton-System** garbage collector.

.. note::
    Finished tasks are removed by GC as soon as possible, so don't check whether task is finished only by checking its state. Non-existing tasks should be also treated as possibly finished (and collected).

Consumer queue persistence
--------------------------

Consumer queue is created on the first registration of consumer and it gets new tasks even if all consumer instances are offline. It guarantees that analysis will complete even after short downtime of part of subsystems. Unfortunately, it also blocks completion of the analysis when we connect a Kartonik which is currently developed or temporary.

We can turn off queue persistence using the :code:`persistent = False` attribute in the Karton subsystem class.

.. code-block:: python

    class TemporaryConsumer(Karton):
        identity = "karton.temporary-consumer"
        filters = ...
        persistent = False

        def process(self):
            ...

This is also the (hacky) way to remove persistent queue from the system. Just launch empty consumer with identity you want to remove, wait until all tasks will be consumed and shut down the consumer.

.. code-block:: python

    from karton.core import Karton

    class DeleteThisConsumer(Karton):
        identity = "karton.identity-to-be-removed"
        filters = {}
        persistent = False

        def process(self):
            pass

    DeleteThisConsumer().loop()

Prioritized tasks
-----------------

Karton allows to set priority for task tree: :attr:`TaskPriority.HIGH`, :attr:`TaskPriority.NORMAL` (default) or :attr:`TaskPriority.LOW`. Priority is determined by producer spawning an initial task.

.. code-block:: python

    producer = Producer()
    task = Task(
        headers=...,
        priority=TaskPriority.HIGH
    )
    producer.send_task(task)

All tasks within the same task tree have the same priority, which is derived from the priority of initial task. If consumer will try to set different priority for spawned tasks, new priority settings will be simply ignored.

Extending configuration
-----------------------

During processing we may need to fetch data from external service or use libraries that need to be pre-configured. Most simple approach is to use separate configuration file, but this is a bit messy.

Karton configuration is represented by special object :class:`karton.Config`, which can be explicitly provided as an argument to the Karton constructor. `Config` is based on :class:`configparser.ConfigParser`, so we can extend it with additional sections for custom configuration.

For example, if we need to communicate with Malwarecage, we can make Malwarecage binding available via :code:`self.config.mwdb`

.. code-block:: python

    class MWDBConfig(Config):
        def __init__(self, path=None) -> None:
            super().__init__(path)
            self.mwdb_config = dict(self.config.items("mwdb"))

        def mwdb(self) -> Malwarecage:
            mwdb = Malwarecage(
                api_key=self.mwdb_config.get("api_key"),
                api_url=self.mwdb_config.get("api_url", mwdblib.api.API_URL)
            )
            if not api.api_key:
                mwdb.login(
                    self.mwdb_config["username"],
                    self.mwdb_config["password"]
                )
            return mwdb

    class GenericUnpacker(Karton):
        ...

        def process(self):
            file_hash = self.current_task.get_payload("file_hash")
            sample = self.config.mwdb.query_file(file_hash)

    if __name__ == "__main__":
        GenericUnpacker(MWDBConfig()).loop()

and provide additional section in `karton.ini` file:

.. code-block::

   [minio]
   secret_key = <redacted>
   access_key = <redacted>
   address = 127.0.0.1:9000
   bucket = karton
   secure = 0

   [redis]
   host = 127.0.0.1
   port = 6379

   [mwdb]
   api_url = http://127.0.0.1:5000/api
   api_key = <redacted>

Karton-wide and instance-wide configuration
```````````````````````````````````````````

By default - configuration is searched in the following locations (by searching order):

- :code:`/etc/karton/karton.ini`
- :code:`~/.config/karton/karton.ini`
- :code:`./karton.ini`
- environment variables

Each next level overrides and merges with the values loaded from the previous path. It means that we can provide karton-wide configuration and specialized instance-wide extended configuration specific for subsystem.

Contents of :code:`/etc/karton/karton.ini`:

.. code-block::

   [minio]
   secret_key = <redacted>
   access_key = <redacted>
   address = 127.0.0.1:9000
   bucket = karton
   secure = 0

   [redis]
   host = 127.0.0.1
   port = 6379

and specialized configuration in the working directory :code:`./karton.ini`

.. code-block::

   [mwdb]
   api_url = http://127.0.0.1:5000/api
   api_key = <redacted>

Passing tasks to the external queue
-----------------------------------

Karton can be used to delegate tasks to separate queues e.g. external sandbox. External sandboxes usually have their own concurrency and queueing mechanisms, so Karton subsystem needs to:

- dispatch task to the external service
- wait until service ends processing
- fetch results and spawn result tasks keeping the `root_uid` and `parent_uid`

There multiple approaches to do that.

Busy waiting
````````````
The most simple way to do that is to perform all of these actions synchronously, inside the :meth:`process` method.

.. code-block:: python
    
    def process(self):
        sample = self.current_task.get_resource("sample")
        
        # Dispatch task, getting the analysis_id
        with sample.download_temporary_file() as f:
            analysis_id = sandbox.push_file(f)
        
        # Wait until analysis finish
        while sandbox.is_finished(analysis_id):
            # Check every 5 seconds
            time.sleep(5)

        # If analysis has been finished: get the results and process them
        analysis = sandbox.get_results(analysis_id)
        self.process_results(analysis)


This approach has few disadvantages:

- if our sandbox supports concurrency, we need to spawn as much consumers as we need to. It's memory-consuming and these consumers are just waiting for most of the time;
- when consumer has been terminated during tracking task status, it can't recover from that. Task will be orphaned and results will stay unreported;

Asynchronic tasks
`````````````````

Another, experimental approach are **asynchronic tasks**, allowing us to get out of :meth:`process` method without setting task status to finished. To handle that, we need to split our subsystem to two parts: dispatcher and status tracker.

Dispatcher part can look similar to this:

.. code-block:: python

    class SandboxDispatcher(Karton):
        identity = "karton.sandbox-analyzer"
        
        filters = ...

        def process(self):
            sample = self.current_task.get_resource("sample")
        
            # Dispatch task, getting the analysis_id
            with sample.download_temporary_file() as f:
                analysis_id = sandbox.push_file(f)
                # Mark task as asynchronic, so it won't be finished
                # after we go out of self.process() method
                self.current_task.make_asynchronic()
                # Store tracking information
                self.rs.hsetnx("sandbox-tasks", analysis_id, self.current_task.uid)
                
Status tracker part:

.. code-block:: python

    class SandboxResultProcessor(Karton):
        identity = "karton.sandbox-analyzer"

        filters = ...

        # We need to provide the same identity and filters!
    
        def process(self):
            analysis = sandbox.get_results(self.analysis_id)
            self.process_results(analysis)
            # Asynchronic state is not stored anywhere, so task will 
            # just finish in tracker context

    sandbox_processor = SandboxResultProcessor()

    while True:
        for analysis_id in redis_queue.hkeys("sandbox-tasks"):
            task_uid = self.rs.hget("sandbox-tasks", analysis_id)
            # This will fetch the task_uid from Redis and restart processing
            # inside our SandboxResultProcessor
            sandbox_processor.analysis_id = analysis_id
            sandbox_processor.internal_process(task_uid)
        time.sleep(5)
            

But this approach is still (very, very) far from ideal:

- hacking your own library is never a good idea (:at_least_not_yet:)
- all incoming tasks in SandboxDispatcher are immediately started, even if they are waiting in sandbox queue. This means that we can't reasonably track processing time and there is a risk that task will be prematurely terminated. We need to limit number of fetched tasks by SandboxDispatcher to the concurrency limit provided by sandbox (e.g. using counting Redis-based semaphore)

So... this approach is not completely bad, but it needs to be more supported by Karton library. TODO.