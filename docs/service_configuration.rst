Configuration and customization
===============================

This chapter describes how to configure and customize Karton services, including ready-made ones available on PyPi/Github.

Karton services can be configured in various ways. Let's take a look at basic configuration.

.. code-block::

   [gateway]
   url = http://gateway-server:8000/
   auth_tokens = ey..

Configuration values are read from various sources using the following precedence:

- ``/etc/karton/karton.ini`` file (global)
- ``~/.config/karton/karton.ini`` file (user local)
- ``./karton.ini`` file (subsystem local)
- path provided via ``KARTON_CONFIG_FILE`` environment variable
- ``--config-file <path>`` optional, additional path provided in arguments
- ``KARTON_SECTION_OPTION`` values from environment variables e.g. the ``auth_tokens`` option in the ``[gateway]`` section can be overridden using the ``KARTON_GATEWAY_AUTH_TOKENS`` variable
- Command-line arguments (if ``Karton.main()`` method is used as an entrypoint)

You can build your configuration hierarchically e.g. by providing common settings in ``/etc/karton/karton.ini``, service-specific settings in local ``./karton.ini`` and secrets in env vars.

Common Karton configuration fields are listed below:

=============   =======================   =====================================================================================================================================================
 Section         Option                     Description
=============   =======================   =====================================================================================================================================================
 [gateway]       url                        Karton Gateway server URL
 [gateway]       auth_tokens                Karton authentication tokens. Multiple tokens can be provided, separated by comma ","
 [gateway]       secondary_auth_tokens      Karton secondary authentication tokens. Appended to auth_tokens.
 [gateway]       s3_hostname_override       Optional hostname override for presigned S3 resource URLs returned by Gateway server
 [gateway]       retries                    Number of connection retries before failing (default: 5)
 [gateway]       retry_base_timeout         Retry base timeout for exponential backoff in seconds (default: 2)
 [gateway]       retry_jitter               Retry jitter range for random delay added to the base timeout to avoid simultaneous connection tries after Gateway server restart (default: 1)
 [gateway]       connect_timeout            Timeout for single connection try into websocket in seconds (default: 5)
 [gateway]       response_timeout           Timeout for reading a message from websocket in seconds (default: 15)
 [karton]        identity                   Karton service identity override (overrides the name provided in class / constructor arguments)
 [karton]        persistent                 Karton service queue persistency override
 [karton]        debug                      Karton debug mode for service development
 [karton]        task_timeout               Karton service task execution timeout in seconds. Useful if your service sometimes hangs. Karton will schedule SIGALRM if this value is set.
 [logging]       level                      Logging level for Karton service logger (default: INFO)
 [logging]       enable_publish             Turns on publishing of service log records to the 'karton.logs' queue (default: on)
 [signaling]     status                     Turns on producing of 'karton.signaling.status' tasks, signalling the task start and finish events by Karton service (default: 0, off)
=============   =======================   =====================================================================================================================================================

Core services configuration (S3, Redis)
---------------------------------------

Core services (Karton System and Karton Gateway) require direct S3 and Redis configuration.

.. code-block:: ini

    [s3]
    secret_key = minioadmin
    access_key = minioadmin
    address = http://localhost:9000
    bucket = karton

    [redis]
    host = localhost
    port = 6379

Configuration values are read from various sources using the following precedence:

- ``/etc/karton/karton.ini`` file (global)
- ``~/.config/karton/karton.ini`` file (user local)
- ``./karton.ini`` file (subsystem local)
- path provided via ``KARTON_CONFIG_FILE`` environment variable
- ``--config-file <path>`` optional, additional path provided in arguments
- ``KARTON_SECTION_OPTION`` values from environment variables e.g. the ``secret_key`` option in the ``[s3]`` section can be overridden using the ``KARTON_S3_SECRET_KEY`` variable
- Command-line arguments (if ``Karton.main()`` method is used as entrypoint)

You can build your configuration hierarchically e.g. by providing common settings in ``/etc/karton/karton.ini``, service-specific settings in local ``./karton.ini`` and secrets in env vars.

Common Karton configuration fields are listed below:

============   ===============   =======================================================================================================================================
 Section        Option            Description
============   ===============   =======================================================================================================================================
 [s3]           address           S3 API address
 [s3]           access_key        S3 API access key (username)
 [s3]           secret_key        S3 API secret key (password)
 [s3]           iam_auth          S3 IAM configuration (default: False)
 [s3]           bucket            Default bucket name for storing produced resources
 [redis]        host              Redis server hostname
 [redis]        port              Redis server port
 [redis]        db                Redis server database id (default: 0)
 [redis]        username          Redis server AUTH username (default: None)
 [redis]        password          Redis server AUTH password (default: None)
 [redis]        socket_timeout    Socket timeout for Redis operations in seconds (default: 30, use 0 to turn off if timeout doesn't work properly)
 [redis]        ssl               Use TLS for Redis connection (default: 0)
 [redis]        url               Redis server url (allows passing custom arbitrary options)
============   ===============   =======================================================================================================================================

Note that if both ``iam_auth = True`` and the ``access_key``, ``secret_key`` pair are provided in the configuration file, Karton will first try to load secrets via IAM provider and
will fallback to the provided pair otherwise. More information about credential loading can be found `here <https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html#configuring-credentials>`_.

Karton System configuration
---------------------------

Most core services can be tuned depending on your needs. Custom service configuration is handled the same way as general Karton configuration.

A good example is Karton System:

============   ================================   ==============================================================================================================================================================================
 Section        Option                             Description
============   ================================   ==============================================================================================================================================================================
 [system]        gc_interval                       Spawn interval for garbage collection tasks in seconds (default: 3 minutes)
 [system]        task_dispatched_timeout           Timeout for tasks that are stuck in DISPATCHED state (e.g. Producer crashed during upload of resources) (default: 24 hours)
 [system]        task_started_timeout              Timeout for tasks that are stuck in STARTED state (e.g. non-graceful crash of Consumer during task processing) (default: 24 hours)
 [system]        task_crashed_timeout              Timeout for removal of crashed tasks (default: 3 days)
 [system]        enable_gc                         Enable garbage collection. GC can be turned off if you want to scale up routing using several Karton System instances (default: on)
 [system]        enable_router                     Enable task routing. Routing can be turned off if you want to use dedicated Karton System instance for GC (default: on)
 [system]        crash_started_tasks_on_timeout    Enable crashing started tasks when task_started_timeout is reached instead of deleting them (introduced in v5.6.0)
 [system]        enable_null_version_deletion      Enable deletion of "null" version from S3, which is needed in some S3 providers (e.g. MinIO) to correctly remove resources when versioning is suspended (introduced in v5.6.1)
 [system]        enable_task_tracking              Enable tracking of task connections between producers and consumers (default: on)
 [system]        task_tracking_ttl                 Time-to-live for task tracking entries in seconds (default: 30 days)
============   ================================   ==============================================================================================================================================================================

All settings can also be set using command-line arguments.

.. code-block:: console

    $ karton-system --help
    usage: karton-system [-h] [--version] [--config-file CONFIG_FILE]
                         [--identity IDENTITY] [--log-level LOG_LEVEL] [--debug]
                         [--setup-bucket] [--disable-gc] [--disable-router]
                         [--gc-interval GC_INTERVAL]
                         [--task-dispatched-timeout TASK_DISPATCHED_TIMEOUT]
                         [--task-started-timeout TASK_STARTED_TIMEOUT]
                         [--task-crashed-timeout TASK_CRASHED_TIMEOUT]
                         [--crash-started-task-on-timeout]

    Karton message broker.

    options:
      -h, --help            show this help message and exit
      --version             show program's version number and exit
      --config-file CONFIG_FILE
                            Alternative configuration path
      --identity IDENTITY   Alternative identity for Karton service
      --log-level LOG_LEVEL
                            Logging level of Karton logger
      --debug               Enable debugging mode
      --setup-bucket        Create missing bucket in S3
      --disable-gc          Do not run GC in this instance
      --disable-router      Do not run task routing in this instance
      --gc-interval GC_INTERVAL
                            Garbage collection interval
      --task-dispatched-timeout TASK_DISPATCHED_TIMEOUT
                            Timeout for non-enqueued tasks stuck in Dispatched
                            state (non-graceful shutdown of producer)
      --task-started-timeout TASK_STARTED_TIMEOUT
                            Timeout for non-enqueued tasks stuck in Started
                            state (non-graceful shutdown of consumer)
      --task-crashed-timeout TASK_CRASHED_TIMEOUT
                            Timeout for tasks in Crashed state
      --crash-started-task-on-timeout
                            Crash Started tasks on timeout instead of deleting

Karton Gateway Server configuration
-----------------------------------

Karton Gateway Server can be configured using the ``[gateway-server]`` configuration section.

==================       ================================       =================================================================================================================================================================================
 Section                 Option                                 Description
==================       ================================       =================================================================================================================================================================================
 [gateway-server]        secret_key                             Secret key used for issuing and validating task tokens. Random string of at least 64 characters. Required.
 [gateway-server]        auth_required                          If set to "1" (default), service authorization is required. Can be set to "0" to disable authorization.
 [gateway-server]        jwks_path                              Alternative path to jwks.json file (default is ./jwks.json)
 [gateway-server]        allowed_foreign_buckets                S3 bucket names other than default [s3] bucket one, that can be referenced by tasks. S3 access key must have access to these buckets. Accepts multiple names, separated by comma.
==================       ================================       =================================================================================================================================================================================

.. _extending-config:

Extending configuration
-----------------------

During development of your own Karton services you may want to provide your own configuration fields.

All configuration values set in ``karton.ini`` files and ``KARTON_`` envs are available in ``self.config`` object and don't
require additional definition.

The only thing that needs to be extended is the argument parser if you want to use command-line arguments. Fortunately,
Karton classes expose dedicated methods for this purpose.

.. code-block:: python

    import argparse

    from karton.core import Config, Karton, Task

    class SmolKarton(Karton):
        identity = "karton.smol"
        filters = [{
            "type": "smol-tasks"
        }]

        def process(self, task: Task) -> None:
            if self.config.has_option("smol", "how_smol"):
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
                default=1024,
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

.. warning::

    Watch out for sections with a ``-`` character when configuring karton services using environment variables.
    Some shells (like /bin/sh) won't work correctly and configuration will not be populated with your settings.
    See `karton/issues/235 <https://github.com/CERT-Polska/karton/issues/235>`_ for more info.

Customizing ready-made Karton services
--------------------------------------

Ready-made Karton services like ``karton-mwdb-reporter`` come with a predefined set of filters and emitted headers.
If you want to extend them or override them without forking the whole project, you can simply extend the Karton class
and override things you need.

.. code-block:: python

    from karton.mwdb_reporter import MWDBReporter

    class CustomMWDBReporter(MWDBReporter):
        filters = [
            *MWDBReporter.filters,
            {"type": "sample", "stage": "my-stage"}
        ]

    if __name__ == "__main__":
        CustomMWDBReporter.main()

.. warning::

    It's recommended to pin to the specific version of service you derive from in case of conflicting changes.
