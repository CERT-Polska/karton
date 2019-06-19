Examples
==================================
Here are few examples of usage.

Producer
-------------------

.. code-block:: python

    import sys
    from karton import Config, Producer, Task, Resource

    config = Config(os.path.join(os.path.dirname(__file__), "config.ini"))
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
    from karton import Config, Consumer, Task, Resource

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

        def process(self):
           if self.current_task.headers["type"] == "sample":
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


`self.current_task.headers` gives you information on why task was routed and methods like `get_resource` or `get_payload` allow you to get resources or metadata from task.

Finally, we need to run our module, we get this done with `loop` method, which blocks on listening for new tasks, running `process` when needed.

.. code-block:: python

    if __name__ == "__main__":
        config = Config(os.path.join(os.path.dirname(__file__), "config.ini"))
        c = Reporter(config)
        c.loop()


Karton
-------------------
