.. karton documentation master file, created by
   sphinx-quickstart on Fri Mar 22 17:40:08 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to karton's documentation!
==================================
Karton is a library made for analysis backend implementation.
Allows you to attach new systems (kartoniki) to the pipeline with ease.

This is achieved by combining powers of a few existing solutions, karton just glues them together and allows you to have some sane amount of abstraction over them.

Karton ecosystem consists of:

- `RabbitMQ <https://www.rabbitmq.com/>`_ - core of the system, allows for routing of the tasks and communication between them.

- `Minio <https://github.com/minio/minio>`_ - temporary object storage compatible with Amazon S3 API, holds all the heavy objects, everything that is too big to be fit in RMQ goes through there.

- `Splunk <https://www.splunk.com/>`_ - holds all of the logs, random messages from systems as well as operational logs used for showing information about running tasks or task flows inside the system.


**Task** routing and exchange is achieved with the help of **RabbitMQ**.

All of the tasks consist of `headers`, `resources` and `payload` information.

`Headers` are used in the routing process, these help to determine which system is interested in which task.

`Resources` are files, directories, samples, analysis, everything that we want to analyze inside the whole network of systems.

`Payload` contains metadata about the task. Some examples are `tags` - which we can store inside mwdb, or `additional_info` which we save as comments for now.



.. toctree::
   :maxdepth: 2
   :caption: Examples:

   examples

.. toctree::
   :maxdepth: 2
   :caption: Karton interface reference:

   karton

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
