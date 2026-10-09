Breaking changes
================

This chapter will describe significant changes introduced in major version releases of Karton. Versions before 4.0.0 were not officially released, so they have value only for internal purposes. Don't worry about it if you are a new user.

What is changed in Karton 6.0.0
-------------------------------

As in the 5.x release: Karton-System and core services are still able to communicate with previous versions.
Karton service code shouldn't require any changes to migrate to 6.x. The breaking changes are internal,
so they may require attention only if your code operates directly on the Karton Backend.

Version 6.0.0 introduces a new type of backend API called the Karton Gateway API. Karton services communicate via
a Websocket/REST API server (called the Karton Gateway Server) instead of direct calls to the Redis database. Resources
are exchanged via S3 presigned URLs issued by the Karton Gateway, so no direct S3 authentication is needed.

Karton Gateway is considered the primary backend going forward. The Redis-based interface (called "Direct backend")
still works but is considered legacy, and users are encouraged to migrate to the Gateway-based setup.

The Karton Gateway Server is a separate service shipped within this package and runs as a FastAPI/gunicorn
application. Install it together with the extra dependencies::

    pip install karton-core[gateway]


Python and dependencies
^^^^^^^^^^^^^^^^^^^^^^^

* Python 3.12 or newer is now required.
* New runtime dependencies were added: ``httpx2``, ``pydantic`` and ``websockets`` to support the Gateway
  backend implementation. The Gateway Server additionally requires ``fastapi``, ``pyjwt`` and ``gunicorn``
  (provided by the ``gateway`` extra).

Configuration
^^^^^^^^^^^^^

* The ``[gateway]`` configuration section selects the Gateway backend. It is **mutually exclusive** with the
  ``[redis]``, ``[minio]`` and ``[s3]`` sections: defining ``[gateway]`` alongside any of them raises a
  ``RuntimeError`` at startup. A single ``karton.ini`` must use either the Gateway or the Direct backend,
  not both.

  Client-side ``[gateway]`` section:

  .. code-block:: ini

      [gateway]
      url = ws://gateway.example.com:8000/gateway
      password = ...
      ; optional:
      s3_hostname_override = gateway.example.com:9000
      retries = 5
      retry_base_timeout = 2
      retry_jitter = 1
      connect_timeout = 5
      response_timeout = 5

  Server-side ``[gateway-server]`` section (used by the Gateway Server process):

  .. code-block:: ini

      [gateway-server]
      secret_key = ...    ; at least 32 characters
      password = ...      ; at least 16 characters
      allowed_buckets = bucket-one,bucket-two   ; comma-separated, not JSON

Backend and service identity
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* Backends should now be obtained via the :func:`get_backend` factory instead of being constructed
  directly. ``get_backend(config, service_info)`` inspects the configuration and returns the matching
  implementation: a ``KartonGatewayBackend`` when a ``[gateway]`` section is present, or a
  ``KartonBackend`` (Direct) otherwise. ``KartonBase`` and ``KartonAsyncBase`` already do this
  internally via the ``_backend_factory`` class attribute, so most services need no change.

  .. code-block:: python

      from karton.core.backend import get_backend, KartonServiceInfo

      service_info = KartonServiceInfo.create(
          identity="karton.my-service",
      )
      # Preferred: let the factory pick the right backend for the configuration
      backend = get_backend(config, service_info=service_info)

* ``karton.core.backend`` has been turned from a single module into a package. The Direct backend class
  now lives in ``karton.core.backend.direct`` (``KartonBackend`` remains re-exported from
  ``karton.core.backend`` for backwards compatibility). New names exported from the package:

  - ``KartonBackendProtocol`` – a :class:`typing.Protocol` describing the backend interface used by
    producers and consumers, so they no longer depend on a concrete implementation. Code that accepts a
    backend should type it as ``KartonBackendProtocol`` rather than the concrete ``KartonBackend``.
  - ``KartonGatewayBackend`` – the Gateway-backed implementation of the protocol.

* Constructing a backend directly (e.g. ``KartonBackend(config, service_info=...)``) is still possible
  but discouraged in favor of :func:`get_backend`. If you do construct one directly, note that the
  ``KartonBackend.__init__`` signature changed: the ``identity`` argument was removed in favor of the
  now-**required** ``service_info``:

  .. code-block:: diff

      - backend = KartonBackend(config, identity="karton.my-service")
      + backend = get_backend(
      +     config,
      +     service_info=KartonServiceInfo.create(
      +         identity="karton.my-service",
      +     ),
      + )

  In practice, you rarely construct a backend or ``service_info`` yourself: ``KartonBase`` /
  ``KartonAsyncBase`` build ``service_info`` from the service ``identity`` and ``version``
  automatically. If you pass a custom ``backend`` to a service constructor, it must now be a
  ``KartonBackendProtocol`` rather than the concrete ``KartonBackend`` class.

* ``KartonServiceInfo`` gained a **required** ``instance_id`` field (a unique identifier per running
  service instance) used to deduplicate connections from the same replica, and now validates the
  ``identity`` (raises :class:`karton.core.exceptions.InvalidIdentityError` on empty strings or
  characters from ``{" ", "?"}``). Use the :meth:`KartonServiceInfo.create` classmethod to
  construct one with a randomly generated ``instance_id`` instead of passing it manually.

* The ``with_service_info`` class attribute on ``KartonBase``/``KartonAsyncBase`` has been removed.
  Service information is always populated, so enabling it explicitly is no longer necessary. The
  ``resolve_service_info()`` helper has been removed as well.

* The asyncio backend was restructured to mirror the sync one: ``karton.core.asyncio.backend`` is now a
  package (``backend/{base,direct,gateway}.py``). ``karton.core.asyncio.KartonGatewayBackend`` was
  renamed to ``KartonAsyncGatewayBackend``, and a matching ``get_backend()`` factory and
  ``KartonAsyncBackendProtocol`` are exported from ``karton.core.asyncio.backend``.

* ``KartonState.replicas`` is now populated via :meth:`get_online_identities`, which returns a
  ``Dict[str, List[KartonExternalServiceInfo]]``. :class:`KartonExternalServiceInfo` is a new dataclass
  representing services observed online (parsed from Redis/Gateway), distinct from
  :class:`KartonServiceInfo`, which describes the local service. The previous
  :meth:`get_online_consumers` behavior (returning ``Dict[str, List[Dict[str, str]]]``) is superseded by
  this typed representation.

Resources
^^^^^^^^^

* :meth:`RemoteResource.remove` has been removed.

* Backend object-storage methods were renamed to operate on resource objects rather than
  ``(bucket, uid)`` pairs:

  .. code-block:: diff

      - backend.upload_object(bucket, uid, content)
      + backend.upload_resource(resource, content)
      - backend.upload_object_from_file(bucket, uid, path)
      + backend.upload_resource_from_file(resource, path)
      - backend.download_object(bucket, uid)
      + backend.download_resource(resource)
      - backend.download_object_to_file(bucket, uid, path)
      + backend.download_resource_to_file(resource, path)

  The public :meth:`Resource.upload`, :meth:`RemoteResource.download` and
  :meth:`RemoteResource.download_to_file` APIs are unchanged; only custom backend subclasses are
  affected.

* :meth:`RemoteResource.download` and :meth:`RemoteResource.download_to_file` no longer raise
  ``"bucket is not set"`` — bucket resolution is now the backend's responsibility, which is required for
  the Gateway backend where resources are fetched via presigned URLs.

Consumer behavior
^^^^^^^^^^^^^^^^^

* Bind-change detection no longer relies on polling :meth:`get_bind` against the local ``self._bind``.
  Instead, :meth:`consume_routed_task` raises the new :class:`karton.core.exceptions.BindExpiredError`
  when the bind has been overridden by a newer service version. Consumers catch this to shut down
  gracefully. Custom backend subclasses implementing ``consume_routed_task`` should raise
  ``BindExpiredError`` accordingly.

Minor changes
^^^^^^^^^^^^^

* :meth:`KartonBackend.get_bind` now returns ``None`` when a bind is not found, instead of raising an
  exception while unserializing a missing entry. Callers should handle the ``None`` case.
* :class:`KartonBind` is now a frozen :class:`dataclasses.dataclass` instead of a ``collections.namedtuple``.
  Field access by name is unchanged, but code that relied on the namedtuple's tuple interface
  (e.g. positional unpacking) needs to be updated.


What is changed in Karton 5.0.0
-------------------------------

Karton-System and core services are still able to communicate with previous versions.

* Changed name of ``karton.ini`` section that contains S3 client configuration from ``[minio]`` to ``[s3]``.

  In addition to this, you need to add a URI scheme to the ``address`` field and remove the ``secure`` field.
  If ``secure`` was 0, correct scheme is ``http://``. If ``secure`` was 1, use ``https://``.

  .. code-block:: diff

    - [minio]
    + [s3]
      access_key = karton-test-access
      secret_key = karton-test-key
    - address = localhost:9000
    + address = http://localhost:9000
      bucket = karton
    - secure = 0

  v5.0.0 maps ``[minio]`` configuration to correct ``[s3]`` configuration internally, but ``[minio]`` scheme
  is considered deprecated and can be removed in further major release.

* Karton library uses `Boto3 <https://github.com/boto/boto3>`_ library as a S3 client instead of `Minio-Py <https://github.com/minio/minio-py>`_ underneath.
  You may want to check if your code relies on exceptions thrown by previous S3 client.

* :class:`karton.core.Config` interface is changed. ``config``, ``minio_config`` and ``redis_config`` attributes are no longer available.

* We noticed lots of issues caused by calling factory method ``main()`` on instance instead of class, which can be misleading (:py:meth:``karton.core.base.KartonBase.main``
  actually creates own instance of Karton service internally, so the initialization is doubled). To notice these errors more quickly, we prevented ``main()`` call on ``KartonBase`` instance

  .. code-block:: python

    if __name__ == "__main__":
        MyConsumer.main()  # correct

    if __name__ == "__main__":
        MyConsumer().main()  # throws TypeError

* ``karton.core.Consumer.process`` no longer accepts no arguments. First argument of this method is the incoming task.

.. code-block:: python

    # Correct
    class MyConsumer(Karton):
        def process(self, task: Task) -> None:
            ...

    # Wrong from v5.0.0
    class MyConsumer(Karton):
        def process(self) -> None:
            ...


What is changed in Karton 4.0.0
-------------------------------

Karton-System and core services are still compatible with both 3.x and 2.x versions.

* ``SHA256`` is evaluated always when :class:`Resource` is created. If you already know it and don't want it to be recalculated, pass the hash to the constructor via ``sha256=`` argument.
  
  .. code-block:: python

    sample = Resource(path="sample.exe", sha256="2e5d...")

* :class:`DirectoryResource` has been removed in favor of :class:`Resource.from_directory`. Resources created using this method are still deserialized to the :class:`RemoteDirectoryResource` form
  by older Karton versions. :class:`RemoteDirectoryResource` has been merged into :class:`RemoteResource`, so all resources containing Zip files can be unzipped even if they were created as regular files.

* Asynchronous tasks has been removed. Busy waiting should be used instead.

* All crashed tasks are preserved in ``Crashed`` state until they are removed by Karton-System (default is 72 hours) or retried by user. Keep in mind that they hold all the referenced resources, so keep an eye on that queue.

What is changed in Karton 3.0.0
-------------------------------

Karton-System and other core services in 3.x are compatible with 2.x. But if you want to use 3.x in Karton service code, all core services need to be upgraded first.

The good news:

* Karton subsystems expose the library version and class docstring in :code:`karton.binds`
* Config is explicit and get by default from :code:`karton.ini` file (yup, it's :code:`karton.ini` not :code:`config.ini`). But you can still provide another path if you want.
* There is no need to provide a suffix :code:`".test"` as a part of identity for non-persistent consumer queues. Just set :code:`persistent=False` in your Karton subsystem class
* You can provide :code:`identity` as an argument.

So, instead of that code:

.. code-block:: python

    # Consumer part

    class Subsystem(Karton):
        identity = "karton.subsystem.test"
        filters = {...}

    config = Config("config.ini")
    subsystem = Subsystem(config).loop()

    # Producer part

    class NamedProducer(Producer):
        identity = "karton.named-producer"
    
    config = Config("config.ini")
    producer = NamedProducer(config).send_task(...)

You can write that code:

.. code-block:: python

    # Consumer part

    class Subsystem(Karton):
        identity = "karton.subsystem"
        filters = {...}
        persistent = False

    subsystem = Subsystem().loop()

    # Producer part
    
    producer = Producer(identity="karton.named-producer").send_task(...)


The bad news (for porting):

* Resource classes are completely reworked. 

  * Resources are strictly divided to local (uploadable) and remote (downloadable) ones. The inheritance structure is different than in 2.x, so check the API first.
    
  * There is no :code:`sha256` field, but :code:`metadata` dictionary instead. For compatibility reasons: we expose :code:`sha256` from Karton 2.x as :code:`metadata["sha256"]` and back. New subsystems should not rely on that behavior.
    
  * :code:`flags` are also not exposed.
    
  * Removed :code:`is_directory` method. 
    
    If you need to check whether your resource is directory, use :code:`isinstance(resource, DirectoryResourceBase)` instead.

  * Remote resources are now lazy-objects bound with MinIO, so we can directly get the contents instead of using proxy methods.

    Code from 2.x:

    .. code-block:: python

      sample = self.current_task.get_resource("sample")
      # Calling Consumer method to get local version of resource
      local_sample = self.download_resource(sample)
      # Get the contents
      sample_content = local_sample.content

    must be ported to:

    .. code-block:: python

      sample = self.current_task.get_resource("sample")
      # Contents will be lazy-loaded
      # If you want to download them directly: use sample.download()
      sample_content = sample.content

    All related :class:`Consumer` methods like :meth:`download_resource` or :meth:`download_to_temporary_folder`
    are completely removed. These methods were incomplete and inconsistent, especially for directories. Now, the whole power behind the Resource features is available directly via object methods.

  * Removed :class:`PayloadBag` wrappers with resource iterator methods. They provided additional level of complexity without adding new capabilities. There are classic dictionaries in place of them.

* Task classes also changed a bit

  * :meth:`payload_contains` is renamed to :meth:`has_payload` and doesn't check only non-persistent payload existence, but includes persistent payloads as well.
    
  * :meth:`persistent_payload_contains` is renamed to :meth:`is_payload_persistent`
    
  * :meth:`get_resource` is not just :meth:`get_payload` alias and provides type checking. It does not accept the `default` argument.
    
  * Instead of :meth:`get_resources`, :meth:`get_directory_resources` and :meth:`get_file_resources` - use :meth:`iterate_resources` and do type checking yourself.

* Removed 'kpm' (some kind of helper scripts will be provided in future versions, that one was outdated anyway)
