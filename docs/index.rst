.. karton documentation master file, created by
   sphinx-quickstart on Fri Mar 22 17:40:08 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Karton's documentation!
==================================
Karton is a library made for analysis backend orchestration.
Allows you to build flexible malware analysis pipelines and attach new Karton Services with ease.

This is achieved by combining powers of a few existing solutions, karton just glues them together and allows you to have some sane amount of abstraction over them.

Karton ecosystem consists of:

- `Redis <https://www.redis.io/>`_ - store used for message exchange between Karton subsystems

- Temporary object storage compatible with Amazon S3 API, holds all the heavy objects (aka Resources) like samples, analyses or memory dumps.
  The recommended one is `MinIO <https://github.com/minio/minio>`_.


Task routing and data exchange is achieved with the help of **Karton-System** - core of the Karton, which routes the tasks and keeps everything in order (task lifecycle, garbage collection etc.)

.. code-block:: python

   from karton.core import Karton, Task, Resource

   class GenericUnpacker(Karton):
       """
       Performs sample unpacking
       """
       identity = "karton.generic-unpacker"
       filters = [
           {
               "type": "sample",
               "kind": "runnable",
               "platform": "win32"
           }
       ]

       def process(self, task: Task) -> None:
           # Get sample object
           packed_sample = task.get_resource('sample')
           # Log with self.log
           self.log.info(f"Hi {packed_sample.name}, let me analyze you!")
           ...
           # Send our results for further processing or reporting
           task = Task(
               {
                  "type": "sample",
                  "kind": "raw"
               }, payload = {
                  "parent": packed_sample,
                  "sample": Resource(filename, unpacked)
               })
           self.send_task(task)

   if __name__ == "__main__":
       # Here comes the main loop
       GenericUnpacker.main()


.. toctree::
   :maxdepth: 2
   :caption: Karton reference:

   what_new
   getting_started
   examples
   task_headers_payloads
   advanced_concepts
   unit_tests
   karton_api

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
