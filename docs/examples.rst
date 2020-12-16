Examples
==================================
Here are few examples of common Karton system patterns.

Producer
-------------------

.. code-block:: python

    import sys
    from karton.core import Config, Producer, Task, Resource

    config = Config("karton.ini")
    producer = Producer(config)

    filename = sys.argv[1]
    with open(filename, "rb") as f:
        contents = f.read()

    resource = Resource(os.path.basename(filename), contents)

    task = Task({"type": "sample", "kind": "raw"})

    task.add_resource("sample", resource)
    task.add_payload("tags", ["simple_producer"])
    task.add_payload("additional_info", ["This sample has been added by simple producer example"])

    logging.info('pushing file to karton %s, task %s' % (name, task))
    producer.send_task(task)

   

Consumer
-------------------

Consumer has to define `identity`, name used for identification and binding in RMQ and `filters` - list of dicts determining what tasks system declares it wants to process.

Elements in the list are OR'ed and items inside dicts are AND'ed.


.. code-block:: python

    import sys
    from karton.core import Config, Consumer, Task, Resource

    class Reporter(Consumer):
        identity = "karton.reporter"
        filters = [
            {
                "type": "sample",
                "stage": "recognized"
            },
            {
                "type": "sample",
                "stage": "analyzed"
            },
            {
                "type": "config"
            }
        ]

Above example accepts headers like:

.. code-block:: python

    {
        "type": "sample",
        "stage": "recognized",
        "kind": "runnable",
        "platform": "win32",
        "extension": "jar"
    }

or

.. code-block:: python

    {
        "type": "config",
        "kind": "cuckoo1"
    }


but not 

.. code-block:: python

    {
        "type": "sample",
        "stage": "something"
    }



Next step is to define `process` method, this is handler for incoming tasks that match our filters.

.. code-block:: python

    def process(self, task: Task) -> None:
       if task.headers["type"] == "sample":
           return self.process_sample()
       else:
           return self.process_config()

    def process_sample(self):
        remote_resource = self.current_task.get_resource("sample")
        local_resource = self.download_resource(remote_resource)
        # ...

    def process_config(self):
        config = self.current_task.get_payload("config")
        # ...


``task.headers`` gives you information on why task was routed and methods like `get_resource` or `get_payload` allow you to get resources or metadata from task.

Finally, we need to run our module, we get this done with `loop` method, which blocks on listening for new tasks, running `process` when needed.

.. code-block:: python

    if __name__ == "__main__":
        c = Reporter()
        c.loop()


Karton
-------------------
Karton class is simply Producer and Consumer bundled together.

As defined in `karton/karton.py`:

.. code-block:: python

    class Karton(Consumer, Producer):
    """
    This glues together Consumer and Producer - which is the most common use case
    """

Receiving data is done exactly like in Consumer.
Using producer is no different as well, just use ``self.send_task``.

Full-blown example below.

.. code-block:: python

    from karton.core import Karton, Task

    class SomeNameKarton(Karton):
        # Define identity and filters as you would in the Consumer class
        identity = "karton.somename"
        filters = [
            {
                "type": "config",
            },
            {
                "type": "analysis",
                "kind": "cuckoo1"
            },
        ]

        # Method called by Karton library
        def process(self, task: Task) -> None:
            # Getting resources we need without downloading them locally
            analysis_resource = task.get_resource('analysis')
            config_resource = task.get_resource('config')

            # Log with self.log
            self.log.info("Got resources, lets analyze them!")
            ...

            # Send our results for further processing or reporting
            # Producer part
            t = Task({"type": "sample"})
            t.add_resource("sample", Resource(filename, content))
            self.send_task(task)


.. _example-overriding-config:

Overriding Config
-----------------
Popular use case is to have another custom configuration in addition to the one needed for karton to work.

This can be easily done by overriding `Config` class and using that for `Karton` initialization.

.. code-block:: python

    import mwdblib

    class MWDBConfig(Config):
        def __init__(self, path=None) -> None:
            super().__init__(path)
            self.mwdb_config = dict(self.config.items("mwdb"))

        def mwdb(self) -> mwdblib.MWDB:
            api_key=self.mwdb_config.get("api_key")
            api_url=self.mwdb_config.get("api_url", mwdblib.api.API_URL)

            mwdb = mwdblib.MWDB(api_key=api_key, api_url=api_url)
            if not api_key:
                mwdb.login(
                    self.mwdb_config["username"],
                    self.mwdb_config["password"])
            return mwdb


.. _example-consuming-logs:

Log consumer
------------

By default, all logs created in karton systems are pushed onto a single queue called ``karton.logs`` in the Redis database.

These logs have to be collected in order to prevent our Redis server from blowing up.

This is a very simple example of a system that does that and prints the messages to stderr.


.. code-block:: python

    import sys
    from karton.core.karton import LogConsumer


    class StdoutLogger(LogConsumer):
        identity = "karton.stdout-logger"

        def process_log(self, event: dict) -> None:
            # there are "log" and "operation" events
            if event.get("type") == "log":
                print(f"{event['name']}: {event['message']}", file=sys.stderr)


    if __name__ == "__main__":
        StdoutLogger().loop()
