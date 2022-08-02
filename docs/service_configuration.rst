Configuration and customization
===============================

This chapter describes how to configure and customize Karton services, including ready-made ones available on PyPi/Github.

Basic configuration
-------------------

Karton services can be configured using various ways. Let's take a look at basic configuration.

.. code-block:: ini

    [s3]
    secret_key = minioadmin
    access_key = minioadmin
    address = http://localhost:9000
    bucket = karton

    [redis]
    host=localhost
    port=6379

Configuration values are read from various sources using the following precedence:

- ``/etc/karton/karton.ini`` file (global)
- ``~/.config/karton/karton.ini`` file (user local)
- ``./karton.ini`` file (subsystem local)
- ``--config-path <path>`` optional, additional path provided in arguments
- ``KARTON_SECTION_OPTION`` values from environment variables e.g. (``secret_key`` option in ``[s3]`` section can be overridden using ``KARTON_S3_SECRET_KEY`` variable)
- Command-line arguments (if ``Karton.main()`` method is used as entrypoint)

You can build your configuration hierarchically e.g. by providing common settings in ``/etc/karton/karton.ini``, service-specific settings in local ``./karton.ini`` and secrets in env vars.

Common Karton configuration fields are listed below:

============   ===============   =======================================================================================================================================
 Section        Option                                    Description
============   ===============   =======================================================================================================================================
 [s3]           address           S3 API address
 [s3]           access_key        S3 API access key (username)
 [s3]           secret_key        S3 API secret key (password)
 [s3]           bucket            Default bucket name for storing produced resources
 [redis]        host              Redis server hostname
 [redis]        port              Redis server port
 [redis]        db                Redis server database id (default: 0)
 [redis]        username          Redis server AUTH username (default: None)
 [redis]        password          Redis server AUTH password (default: None)
 [redis]        socket_timeout    Socket timeout for Redis operations in seconds (default: 30, use 0 to turn off if timeout doesn't work properly)
 [karton]       identity          Karton service identity override (overrides the name provided in class / constructor arguments)
 [karton]       persistent        Karton service queue persistency override
 [karton]       task_timeout      Karton service task execution timeout in seconds. Useful if your service sometimes hangs. Karton will schedule SIGALRM if this value is set.
 [logging]      level             Logging level for Karton service logger (default: INFO)
 [signaling]    status            Turns on producing of 'karton.signaling.status' tasks, signalling the task start and finish events by Karton service (default: 0, off)
============   ===============   =======================================================================================================================================


Karton System configuration
---------------------------

Most core services can be tuned depending on your needs. Custom service configuration is handled the same way as general Karton configuration.

Good example is Karton System:

============   =========================   =======================================================================================================================================
 Section        Option                       Description
============   =========================   =======================================================================================================================================
 [system]       gc_interval                  Spawn interval for garbage collection tasks in seconds. Default is 3 minutes.
 [system]       task_dispatched_timeout      Timeout for tasks that are stuck in DISPATCHED state (e.g. Producer crashed during upload of resources). Default is 24 hours.
 [system]       task_started_timeout         Timeout for tasks that are stuck in STARTED state (e.g. non-graceful crash of Consumer during task processing). Default is 24 hours.
 [system]       task_crashed_timeout         Timeout for removal of crashed tasks. Default is 3 days.
 [system]       enable_gc                    Enable garbage collection. GC can be turned off if you want to scale up routing using several Karton System instances.
 [system]       enable_router                Enable task routing. Routing can be turned off if you want to use dedicated Karton System instance for GC.
============   =========================   =======================================================================================================================================

All settings can be set using command-line.

.. code-block:: console

    $ karton-system --help
    usage: karton-system [-h] [--version] [--config-file CONFIG_FILE] [--identity IDENTITY] [--log-level LOG_LEVEL] [--setup-bucket] [--disable-gc] [--disable-router] [--gc-interval GC_INTERVAL]
                         [--task-dispatched-timeout TASK_DISPATCHED_TIMEOUT] [--task-started-timeout TASK_STARTED_TIMEOUT] [--task-crashed-timeout TASK_CRASHED_TIMEOUT]

    Karton message broker.

    options:
      -h, --help            show this help message and exit
      --version             show program's version number and exit
      --config-file CONFIG_FILE
                            Alternative configuration path
      --identity IDENTITY   Alternative identity for Karton service
      --log-level LOG_LEVEL
                            Logging level of Karton logger
      --setup-bucket        Create missing bucket in S3 storage
      --disable-gc          Do not run GC in this instance
      --disable-router      Do not run task routing in this instance
      --gc-interval GC_INTERVAL
                            Garbage collection interval
      --task-dispatched-timeout TASK_DISPATCHED_TIMEOUT
                            Timeout for non-enqueued tasks stuck in Dispatched state (non-graceful shutdown of producer)
      --task-started-timeout TASK_STARTED_TIMEOUT
                            Timeout for non-enqueued tasks stuck in Started state (non-graceful shutdown of consumer)
      --task-crashed-timeout TASK_CRASHED_TIMEOUT
                            Timeout for tasks in Crashed state

.. _extending-config:

Extending configuration
-----------------------

During development of your own Karton services you may want to provide your own configuration fields.

All configuration values set in ``karton.ini`` files and ``KARTON_`` envs are available in ``self.config`` object and don't
require additional definition.

The only thing that needs to be extended is argument parser if you want to use command-line arguments. Fortunately,
Karton classes expose dedicated methods for this purpose.

.. code-block:: python

    import argparse

    from karton import Config, Karton, Task

    class SmolKarton(Karton):
        identity = "karton.smol"
        filters = [{
            "type": "smol-tasks"
        }]

        def process(self, task: Task) -> None:
            if self.config.has_option("smol", "how_smol")
                how_smol = self.config.getint("smol", "how_smol")
                if task.headers["size"] > how_smol:
                   # Task is not smol enough UwU
                   return
            ...

        @classmethod
        def args_parser(cls) -> argparse.ArgumentParser:
            # Remember to call super method to include base arguments
            parser = super().args_parser()
            parser.add_argument(
                "--how-smol",
                type=int,
                default=cls.GC_INTERVAL,
                help="Sets size limit for tasks",
            )
            return parser

        @classmethod
        def config_from_args(cls, config: Config, args: argparse.Namespace) -> None:
            # Remember to call super method to include base arguments
            super().config_from_args(config, args)
            config.load_from_dict(
                {
                    "smol": {
                        "how_smol": args.how_smol,
                    }
                }
            )

    if __name__ == "__main__":
        SmolKarton.main()

``args_parser`` method exposes the ``argparse.ArgumentParser`` that is used for handling CLI arguments. Values from
argparse are then passed to ``config_from_args`` that maps arguments into sections and options of configuration.
That mechanism allows you to define your own arguments and include these values in the final configuration.

Customizing ready-made Karton services
--------------------------------------

Ready-made Karton services like ``karton-mwdb-reporter`` are coming with a predefined set of filters and emitted headers.
If you want to extend them or override them without forking the whole project, you can simply extend the Karton class
and override things you need.

.. code-block:: python

    from karton.mwdb_reporter import MWDBReporter

    class CustomMWDBReporter(MWDBReporter):
        filters = [
            *CustomMWDBReporter,
            {"type": "sample", "stage", "my-stage"}
        ]

    if __name__ == "__main__":
        CustomMWDBReporter.main()

.. warning::

    It's recommended to pin to the specific version of service you derive from in case of conflicting changes.
