Karton API reference
====================

karton.core.Producer, karton.core.Consumer
------------------------------------------

.. automodule:: karton

.. autoclass:: karton.core.Producer
   :members:
   :inherited-members:

.. autoclass:: karton.core.Consumer
   :members:
   :inherited-members:

.. autoclass:: karton.core.Karton
   :members:

karton.core.LogConsumer
-----------------------

.. autoclass:: karton.core.LogConsumer
   :members:

karton.core.Resource
--------------------

.. automodule:: karton.core.resource

.. autoclass:: karton.core.resource.Resource

.. autoclass:: karton.core.resource.LocalResource
   :members:
   :inherited-members:

.. autoclass:: karton.core.resource.RemoteResource
   :members:
   :inherited-members:

karton.core.Task
----------------

.. automodule:: karton.core.task

.. autoclass:: karton.core.task.Task
   :members:


karton.core.Config
------------------

.. autoclass:: karton.core.config.Config
   :members:

[internals] karton.core.backend
-------------------------------

.. autoclass:: karton.core.backend.KartonBackendProtocol
   :members:

.. autofunction:: karton.core.backend.get_backend

[internals] karton.core.asyncio.backend
---------------------------------------

.. autoclass:: karton.core.asyncio.backend.KartonAsyncBackendProtocol
   :members:

.. autofunction:: karton.core.asyncio.backend.get_backend
