Getting started
===============

Installation
------------

You can get the karton framework from pip:

.. code-block:: console

    python -m pip install karton2

Or, if you're feeling adventurous, download the sources using git and install them manually.
In addition to karton, you'll also need to setup `MinIO <https://docs.min.io/docs/minio-quickstart-guide.html>`_ and `Redis-server <https://redis.io/topics/quickstart>`_.


Configuration
-------------

Each karton subsystem needs a `karton.ini` file that contains the connection parameters for Redis and MinIO.

You can also use this file to store custom fields and use them e.g. by :ref:`example-overriding-config`.

By default, the config class will look for the config file in several places, but let's start by placing one in the root of our new karton subsystem.

.. code-block:: ini

    [minio]
    secret_key = minioadmin
    access_key = minioadmin
    address = localhost:9000
    bucket = karton

    [redis]
    host=localhost
    port=6379

If everything was configured correctly, you should now be able to run the `karton-system` broker and get `Manager karton.system started` signaling that it's able to connect to Redis and MinIO correctly.


Writing your first Producer and Consumer
----------------------------------------

Since all great examples start with foobar, that's exactly what we're going to do.
Let's start by writing a producer that spawns new tasks.

.. code-block:: python

    from karton.core import Producer, Task

    if __name__ == "__main__":
        foo_producer = Producer(identity="foobar-producer")
        for i in range(5):
            task = Task(headers={"type": "foobar"}, payload={"n": i})
            foo_producer.send_task(task)


That was pretty short! Now for a bit longer consumer:

.. code-block:: python

    from karton.core import Consumer, Task

    class FooBarConsumer(Consumer):
        identity = "foobar-consumer"
        filters = [
            {
                "type": "foobar"
            }
        ]
        def process(self, task: Task) -> None:
            num = task.get_payload("data")
            print(num)
            if num % 3 == 0:
                print("Foo")
            if num % 5 == 0:
                print("Bar")

    if __name__ == "__main__":
        FooBarConsumer().loop()

If we now run the consumer and spawn a few "foobar" tasks we should get a few foobars in return:

.. code-block:: none

    [INFO] Service foo-consumer started
    [INFO] Service binds created.
    [INFO] Binding on: {'type': 'foobar'}
    [INFO] Received new task - 884880e0-e5fc-4a71-a93a-08f0caa92889
    0
    Foo
    Bar
    [INFO] Task done - 884880e0-e5fc-4a71-a93a-08f0caa92889
    [INFO] Received new task - 60be2eb5-9e7e-4928-8823-a0d30bbe68ec
    1
    [INFO] Task done - 60be2eb5-9e7e-4928-8823-a0d30bbe68ec
    [INFO] Received new task - 301d8a50-f21e-4e33-b30e-0f3b1cdbda03
    2
    [INFO] Task done - 301d8a50-f21e-4e33-b30e-0f3b1cdbda03
    [INFO] Received new task - 3bb9aea2-4027-440a-8c21-57b6f476233a
    3
    Foo
    [INFO] Task done - 3bb9aea2-4027-440a-8c21-57b6f476233a
    [INFO] Received new task - 050cdace-05b0-4648-a070-bc4a7a8de702
    4
    [INFO] Task done - 050cdace-05b0-4648-a070-bc4a7a8de702
    [INFO] Received new task - d3a39940-d64c-4033-a7da-80eae9786631
    5
    Bar
    [INFO] Task done - d3a39940-d64c-4033-a7da-80eae9786631
