Getting started
===============

Installation
------------

You can get the Karton framework using pip:

.. code-block:: console

    python -m pip install karton-core

Or, if you're feeling adventurous, download the sources using git and install them manually.

In addition to Karton core components, you'll also need to set up S3-compatible storage like `Silo <https://silo.pgsty.com/operations/deployments/installation/>`_ and a `Redis server <https://redis.io/topics/quickstart>`_.

Docker Compose development setup
--------------------------------

Check out the repository called `Karton playground <https://github.com/CERT-Polska/karton-playground/>`_ that provides a similar setup coupled with MWDB Core
and a few open-source Karton services.

If you're just trying Karton out or you want a minimal, quick & easy development environment, check out the ``dev`` folder in the Karton root directory.

It contains a small docker-compose setup that will set up the minimal development environment for you.

All you have to do is run:

.. code-block:: console

    $ docker-compose up --build

And then connect additional Karton systems using the ``karton.ini.dev`` config file.

.. code-block:: console

    $ karton-classifier --config-file dev/karton.ini.dev

If the service has a different hostname for the S3 server than the Gateway server, you may need to configure
an override in the ``karton.ini`` file:

.. code-block:: ini

   [gateway]
   url = http://127.0.0.1:8000/
   auth_tokens = eyJh...
   s3_hostname_override = 127.0.0.1:9000

Core system configuration
-------------------------

Each Karton subsystem needs a ``karton.ini`` file that contains the configuration. Core systems operate directly on Redis and S3, and ``karton.ini`` defines connection parameters for these services.

You can also use this file to store custom fields and use them e.g. by :ref:`extending-config`.

By default, the config class will look for the config file in several places, but let's start by placing one in the root of our new Karton subsystem. The example below defines a minimal setup with a local Redis server and S3-compatible storage:

.. code-block:: ini

    [s3]
    secret_key = minioadmin
    access_key = minioadmin
    address = http://localhost:9000
    bucket = karton

    [redis]
    host = localhost
    port = 6379

If everything was configured correctly, you should now be able to run the ``karton-system`` broker and get ``"Manager karton.system started"`` signaling that it was able to connect to Redis and S3 correctly.

Karton System setup
^^^^^^^^^^^^^^^^^^^

Karton System is a task broker and garbage collector. It moves the tasks from the incoming task queue to the consumer queues and cleans up finished tasks and unreferenced S3 resources.

It can be launched using the ``karton-system`` command installed by the ``karton-core`` package:

.. code-block:: console

    $ karton-system --setup-bucket

The ``--setup-bucket`` option ensures that the ``karton`` S3 bucket is created.

In the default configuration, the ``karton-system`` GC is launched every 3 minutes and routing is temporarily stopped. Garbage collection may take a while when there are multiple pending tasks in the system. To improve the throughput, it is
highly recommended to run separate ``karton-system`` instances for routing and GC:

.. code-block:: console

    # Run router-only system instance
    $ karton-system --disable-gc

    # Run GC-only system instance
    $ karton-system --disable-router

.. _gateway-setup:

Karton Gateway setup
^^^^^^^^^^^^^^^^^^^^

Karton Gateway is an HTTP/Websocket server that exposes Karton API for producers, consumers and other Karton services.

It was implemented in v6.0.0 to avoid direct operations on the Redis database and provide a secure, unified and future-proof interface.

The Gateway server requires an additional ``karton.ini`` configuration section and installation of extra dependencies. First, install the ``karton-core`` package with the ``gateway`` extras:

.. code-block:: console

    $ pip install karton-core[gateway]

Then add an additional ``[gateway-server]`` section to the configuration file and put a random secret key of at least 64 characters there:

.. code-block:: ini

    [gateway-server]
    secret_key = <put secret here>

Then you need to create a key pair that will be used for service authorization. To do this, use the ``karton-auth`` utility.

.. code-block:: console

    $ karton-auth keypair

    Private key passphrase:
    Confirm passphrase:
    Private key written to /home/user/.karton/private.pem
    Public JWKS written to jwks.json


You'll be asked for a passphrase that will be used for the encryption of the private key. Then the private key will be
created in ``~/.karton/private.pem`` and the public key will be written to ``./jwks.json`` in the current directory.

The private key is needed for issuing tokens and authorizing Karton services to perform Karton operations. The public key
(``jwks.json``) needs to be copied to the working directory of the Karton Gateway server and is used for validating
the tokens.

With ``karton.ini`` and ``jwks.json`` in the Karton Gateway server working dir, you can finally run the server:

.. code-block:: console

    $ gunicorn -k karton.gateway.worker.GatewayASGIWorker -w 1 -b 127.0.0.1:8000 karton.gateway:app

Karton Gateway server is based on `FastAPI <https://fastapi.tiangolo.com/>`_ and `Gunicorn <https://gunicorn.org/>`_.

``GatewayASGIWorker`` is extending the native `ASGI worker <https://gunicorn.org/asgi/>`_ with extra shutdown
signalling to ensure the graceful shutdown of all operations in the Gateway.

.. note::

    Service authorization requirement can be turned off by setting the ``auth_required = 0`` option in the ``gateway-server``
    section. It is not recommended in production environments, but may be useful for development and learning the
    ecosystem.

.. _service-configuration:

Service configuration
---------------------

After setting up the core subsystems, we can finally attach an actual working Karton service. Each Karton service
requires:

- configured URL and network access to the Gateway server
- network access to S3 under the same hostname as configured in the Gateway server (presigned resource URLs point there)
- configured authorization token with proper capabilities

Authorization tokens are defined by a JSON-based specification of allowed operations. They can be specified manually or autogenerated by the ``karton-auth`` utility.

First, let's install a basic service e.g. ``karton-classifier``:

.. code-block:: console

    $ pip install karton-classifier

Then we can use ``karton-auth claims`` to autogenerate the claims specification for that service:

.. code-block:: console

    $ karton-auth claims karton.classifier
    {
      "identity": "karton.classifier",
      "allowed_operations": [
        {
          "operation": "produce_task",
          "foreign_buckets": []
        },
        {
          "operation": "consume_task"
        },
        {
          "operation": "register_bind",
          "filters": [
            {
              "type": "sample",
              "kind": "raw"
            }
          ],
          "persistent": true
        }
      ]
    }

As we can see, this specification allows the ``karton.classifier`` to produce tasks, consume tasks and register a
persistent bind (queue) with specified filters. Let's pipe this output to ``karton-auth issue`` to issue a new token:

.. code-block:: console

    $ karton-auth claims karton.classifier | karton-auth issue
    Private key passphrase:
    eyJh...

After entering the passphrase to our private key, we'll get the token on standard output.

Having a generated token, we can create a ``karton.ini`` file for ``karton-classifier``:

.. code-block:: ini

   [gateway]
   url = http://127.0.0.1:8000/
   auth_tokens = eyJh...

Then we're ready to launch the ``karton-classifier``.

Writing your first Producer and Consumer
----------------------------------------

Since all great examples start with foobar, that's exactly what we're going to do.
Let's start by writing a producer that spawns new tasks.

.. code-block:: python

    from karton.core import Producer, Task

    if __name__ == "__main__":
        foo_producer = Producer(identity="karton.foobar-producer")
        for i in range(5):
            task = Task(headers={"type": "foobar"}, payload={"data": i})
            foo_producer.send_task(task)


That was pretty short! Now for a bit longer consumer:

.. code-block:: python

    from karton.core import Consumer, Task

    class FooBarConsumer(Consumer):
        identity = "karton.foobar-consumer"
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

If we now run the consumer and spawn a few "foobar" tasks we should get a few foobar logs in return:

.. code-block:: console

    [INFO] Service karton.foobar-consumer started
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
----------------------------

When you install ``karton-core``, a new command called ``karton`` is added to your terminal.
You can inspect its capabilities by running it:

.. code-block:: console

    (venv) user@computer ~/> karton
    usage: karton [-h] [--version] [-c CONFIG_FILE] [-v] {list,logs,delete,configure} ...

    Karton-core management utility

    positional arguments:
      {list,logs,delete,configure}
                            sub-command help
        list                List active karton binds
        logs                Start streaming logs
        delete              Delete an unused karton bind
        configure           Create a new configuration file

    options:
      -h, --help            show this help message and exit
      --version             show program's version number and exit
      -c, --config-file CONFIG_FILE
                            Alternative configuration path
      -v, --verbose         More verbose log output


The commands are small, utility scripts that are supposed to make maintaining karton a bit easier.

**list**

List active karton consumers. This can be handy if you don't have a dashboard deployed

**logs [--filter FILTER]**

Subscribe to logs coming in from all services. This is very useful if you're trying to hunt down errors or some funky behavior.
You can specify a filter that will limit incoming log messages, for example, to a specific identity - ``--filter "karton.classifier"``.

**delete <identity>**

Delete a persistent queue that's no longer needed.

**configure [--force]**

Create a new ``karton.ini`` configuration file. The config wizard will ask you about various parameters, like the S3 credentials, Redis host, etc. and then save the information into a config file.

