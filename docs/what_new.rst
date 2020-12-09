Breaking changes
================

This chapter will describe significant changes introduced in major version releases of Karton. Versions before 4.0.0 were not officially released, so they have value only for internal purposes. Don't worry about it if you are a new user.

What changed in Karton 4.0.0
----------------------------

Karton-System and core servicies are still compatible with both 3.x and 2.x versions.

* ``SHA256`` is evaluated always when :class:`Resource` is created. If you already know it and don't want it to be recalculated, pass the hash to the constructor via ``sha256=`` argument.
  
  .. code-block:: python

    sample = Resource(path="sample.exe", sha256="2e5d...")

* :class:`DirectoryResource` has been removed in favor of :class:`Resource.from_directory`. Resources created using this method are still deserialized to the :class:`RemoteDirectoryResource` form
  by older Karton versions. :class:`RemoteDirectoryResource` has been merged into :class:`RemoteResource`, so all resources containing Zip files can be unzipped even if they were created as regular files.

* Asynchronic tasks has been removed. Busy waiting should be used instead.

* All crashed tasks are preserved in ``Crashed`` state until they are removed by Karton-System (default is 72 hours) or retried by user. Keep in mind that they hold all the referenced resources, so keep an eye on that queue.

What changed in Karton 3.0.0
----------------------------

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
