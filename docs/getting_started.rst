Getting started
===============

Installation
------------

You can get the Karton framework from pip:

.. code-block:: console

    python -m pip install karton-core

Or, if you're feeling adventurous, download the sources using git and install them manually.

In addition to Karton core library, you'll also need to setup S3-compatible storage like `MinIO <https://docs.min.io/docs/minio-quickstart-guide.html>`_ and `Redis server <https://redis.io/topics/quickstart>`_.


Configuration
-------------

Each Karton subsystem needs a ``karton.ini`` file that contains the connection parameters for Redis and S3.

You can also use this file to store custom fields and use them e.g. by :ref:`extending-config`.

By default, the config class will look for the config file in several places, but let's start by placing one in the root of our new Karton subsystem.

.. code-block:: ini

    [s3]
    secret_key = minioadmin
    access_key = minioadmin
    address = http://localhost:9000
    bucket = karton

    [redis]
    host=localhost
    port=6379

If everything was configured correctly, you should now be able to run the ``karton-system`` broker and get ``"Manager karton.system started"`` signaling that it was able to connect to Redis and S3 correctly.


Docker Compose development setup
--------------------------------

Check out repository called `Karton playground <github.com/CERT-Polska/karton-playground/>`_ that provides similar setup coupled with MWDB Core
and few open-source Karton services.

If you're just trying Karton out or you want a mimimal, quick & easy development environment setup, check out the ``dev`` folder in the Karton root directory.

It contains a small docker-compose setup that will setup the minimal development environment for you.

All you have to do is run

.. code-block:: console

    docker-compose up --build

And then connect additional Karton systems using the ``karton.ini.dev`` config file.

.. code-block:: console

   karton-classifier --config-file dev/karton.ini.dev

Writing your first Producer and Consumer
----------------------------------------

Since all great examples start with foobar, that's exactly what we're going to do.
Let's start by writing a producer that spawns new tasks.

.. code-block:: python

    from karton.core import Producer, Task

    if __name__ == "__main__":
        foo_producer = Producer(identity="foobar-producer")
        for i in range(5):
            task = Task(headers={"type": "foobar"}, payload={"data": i})
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
        FooBarConsumer.main()

If we now run the consumer and spawn a few "foobar" tasks we should get a few foobars logs in return:

.. code-block:: console

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

Check :ref:`service-examples` for more details.

Command-line interface (CLI)
----------------------------------------

When you install ``karton-core``, a new command called ``karton`` is added to your terminal.
You can inspect its capabilities by running it:

.. code-block:: console

    (venv) user@computer ~/> karton
    usage: karton [-h] [--version] [-c CONFIG_FILE] [-v] {list,logs,delete,configure} ...

    Your red pill to the karton-verse

    positional arguments:
    {list,logs,delete,configure}
                            sub-command help
        list                List active karton binds
        logs                Start streaming logs
        delete              Delete an unused karton bind
        configure           Create a new configuration file

    optional arguments:
    -h, --help            show this help message and exit
    --version             show program's version number and exit
    -c CONFIG_FILE, --config-file CONFIG_FILE
                            Alternative configuration path
    -v, --verbose         More verbose log output


The commands are small, utility scripts that are supposed to make maintaining karton a bit easier.

**list**

List active karton consumers, this can be handy if you don't have a dashboard deployed

**logs [--filter FILTER]**

Subscribe to logs coming in from all services. This is very useful if you're trying to hunt down errors or some funky behavior. 
You can specify a filter that will limit incoming log messages, for example, to a specific identity - ``--filter "karton.classifier"``.

**delete <identity>**

Delete a persistent queue that's no longer needed.

**configure [--force]**

Create a new ``karton.ini`` configuration file. The config wizard will ask you about various parameters, like the S3 credentials, Redis host, etc. and then save the information into a config file.

