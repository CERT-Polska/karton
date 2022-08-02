Headers, payloads and resources
=================================

Task consists of two elements: **headers** and **payload**. 

Task headers
------------

Headers specify the purpose of a task and determine how task will be routed by karton-system. They're defined by flat collection of keys and values.

Example:

.. code-block:: python

    task = Task(
        headers = {
            "type": "sample",
            "kind": "runnable",
            "platform": "win32",
            "extension": "dll"
        }
    )

Consumers listen for specific set of headers, which is defined by `filters`.


.. code-block:: python

    class GenericUnpacker(Karton):
        """
        Performs sample unpacking
        """
        identity = "karton.generic-unpacker"
        filters = [
            {
                "type": "sample",
                "kind": "runnable"
            },
            {
                "type": "sample",
                "kind": "script",
                "platform": "win32"
            }
        ]

        def process(self, task: Task) -> None:
            # Get incoming task headers
            headers = task.headers
            self.log.info("Got %s sample from %s", headers["kind"], headers["origin"])

If Karton-System finds that a task matches any of subsets defined by consumer queue filters then the task will be routed to that queue.

Following the convention proposed in examples above, it means that ``GenericUnpacker`` will get all tasks contain samples directly runnable in sandboxes (regardless of target platform) or Windows 32-bit only scripts.

Headers can be used to process our input differently, depending on the kind of sample:

.. code-block:: python

    class GenericUnpacker(Karton):
        ...

        def process(self, task: Task) -> None:
            # Get incoming task headers
            headers = task.headers
            if headers["kind"] == "runnable":
                self.process_runnable()
            elif headers["kind"] == "script":
                self.process_script()

Few headers have special meaning and are added automatically by Karton to incoming/outgoing tasks.

- :code:`{"origin": "<identity>"}` specifies the identity of task sender. It can be used for listening for tasks incoming only from predefined identity.
- :code:`{"receiver": "<identity>"}` is added by Karton when task is routed to the consumer queue. On the receiver side, value is always equal to :code:`self.identity`

Filter patterns
---------------

.. versionadded:: 5.0.0

Filter matching follows two simple rules. If we want task to be routed to the consumer:

- task headers must match **any** of consumer filters
- task headers match consumer filter if they match **all values** defined in filter

Starting from 5.0.0, consumer filters support basic wildcards and exclusions.

========================  ==============================================================================
       Pattern                                           Meaning
------------------------  ------------------------------------------------------------------------------
``{"foo": "bar"}``        matches 'bar' value of 'foo' header
``{"foo": "!bar"}``       matches any value other than 'bar' in 'foo' header
``{"foo": "ba?"}``        matches 'ba' value followed by any character
``{"foo": "ba*"}``        matches 'ba' value followed by any substring (including empty)
``{"foo": "ba[rz]"}``     matches 'ba' value followed by 'r' or 'z' character
``{"foo": "ba[!rz]"}``    matches 'ba' value followed by any character other than 'r' or 'z'
``{"foo": "!ba[!rz]"}``   matches any value of 'foo' header that doesn't match to the "bar[!rz]" pattern
========================  ==============================================================================

Filter logic can be used to fulfill specific use-cases:

====================================  ==============================================================================
   ``filters`` value                                     Meaning
------------------------------------  ------------------------------------------------------------------------------
``[]``                                matches no tasks (no headers allowed). Can be used to turn off queue and consume tasks left.
``[{}]``                              matches any task (no header conditions). Can be used to intercept all tasks incoming to Karton.
``[{"foo": "bar"}, {"foo": "baz"}]``  'foo' header is required and must have 'bar' or 'baz' value.
``[{"foo": "!*"}]``                   'foo' header must be not defined.
====================================  ==============================================================================

.. warning::

    It's recommended to use only strings in filter and header values

    Although some of non-string types are allowed, they will be converted to string for comparison
    which may lead to unexpected results.

Task payload
------------

Payload is also a dictionary, but it's not required to be a flat structure like headers are. Its contents do not affect the routing so task semantics must be defined by headers.

.. code-block:: python

    task = Task(
        headers = ...,
        payload = {
            "entrypoints": [
                "_ExampleFunction@12"
            ],
            "matched_rules": {
                ...
            },
            "sample": Resource("original_name.dll", path="uploads/original_name.dll")
        }
    )

Payload can be accessed by Consumer using :py:meth:`Task.get_payload` method.

.. code-block:: python

    class KartonService(Karton):
        ...
        def process(self, task: Task) -> None:
            entrypoints = task.get_payload("entrypoints", default=[])

But payload dictionary itself still must be **lightweight and JSON-encodable**, because it's stored in Redis along with the whole task definition. 

If task operates on binary blob or complex structure, which is probably the most common use-case, payload can still be used to store the reference to that object. The only requirement is that object must be placed in separate, shared storage, available for both Producer and Consumer. That's exactly how :class:`Resource` objects work.

Resource objects
----------------

Resources are part of a payload that represent a reference to the file or other binary large object. All objects of that kind are stored in S3-compatible storage, which is used as shared object storage between Karton subsystems.

.. code-block:: python

    task = Task(
        headers = ...,
        payload = {
            "sample": Resource("original_name.dll", path="uploads/original_name.dll")
        }
    )

Resource objects created by producer (:class:`LocalResource`) are uploaded to S3 and transformed to :class:`RemoteResource` objects.
RemoteResource is lazy object that allows to download the object contents via :py:attr:`RemoteResource.content` property.


.. code-block:: python

    class GenericUnpacker(Karton):
        ...

        def unpack(self, packed_content: bytes) -> bytes:
            ...

        def process(self, task: Task) -> None:
            # Get sample resource
            sample = task.get_resource("sample")
            # Do the job
            unpacked = self.unpack(sample.content)
            # Publish the results
            task = Task(
                headers={
                    "type": "sample",
                    "kind": "unpacked"
                },
                payload={
                    "sample": Resource("unpacked", content=unpacked)
                }
            )
            self.send_task(task)

If expected resource is too big for in-memory processing or we want to launch external tools that need the file system path, resource contents can be downloaded using :py:meth:`RemoteResource.download_to_file` or :py:meth:`RemoteResource.download_temporary_file`.

.. code-block:: python

    class KartonService(Karton):
        ...
        def process(self, task: Task) -> None:
            archive = task.get_resource("archive")
            with archive.download_temporary_file() as f:
                # f is file-like named object
                archive_path = f.name

If you want to pass original sample along with new task, you can just put a reference back into its payload.

.. code-block:: python

    task = Task(
        headers={
            "type": "sample",
            "kind": "unpacked"
        },
        payload={
            "sample": Resource("unpacked", content=unpacked),
            "parent": sample  # Reference to original (packed) sample
        }
    )
    self.send_task(task)

Each resource has its own metadata store where we can provide additional information about file e.g. SHA-256 checksum

.. code-block:: python
    
    sample = Resource("sample.exe", 
                      content=sample_content,
                      metadata={
                        "sha256": hashlib.sha256(sample_content).hexdigest()
                      })


Starting from v5.0.0, resources can be nested in other objects like lists or dictionaries.

.. code-block:: python

    task = Task(
        headers={
            "type": "analysis",
            "kind": "artifacts"
        },
        payload={
            "artifacts": [
                Resource("file1", content=file1),
                Resource("file2", content=file2),
                Resource("file3", content=file3)
            ]
            "parent": sample  # Reference to original (packed) sample
        }
    )
    self.send_task(task)


More information about resources can be found in API documentation.

Directory resource objects
--------------------------

Resource objects work well for single files, but sometimes we need to deal with bunch of artifacts e.g. process memory dumps from dynamic analysis. Very common way to do that is to pack them into Zip archive using Python `zipfile module <https://docs.python.org/3/library/zipfile.html>`_ facilities.

Karton library includes a helper method for that kind of archives, called :func:`LocalResource.from_directory`.

.. code-block:: python

    task = Task(
        headers={
            "type": "analysis"
        },
        payload={
            "dumps": LocalResource.from_directory(analysis_id, 
                                                  directory_path=f"analyses/{analysis_id}/dumps"),
        }
    )
    self.send_task(task)

Files contained in ``directory_path`` are stored under relative paths to the provided directory path. Default compression level is ``zipfile.ZIP_DEFLATED`` instead of ``zipfile.ZIP_STORED``.

Directory resources are deserialized to the usual :class:`RemoteResource` objects but in contrary to the usual resources they can for example be extracted to directories using :func:`RemoteResource.extract_temporary`

.. code-block:: python

    class KartonService(Karton):
        ...
        def process(self, task: Task) -> None:
            dumps = task.get_resource("dumps")
            with dumps.extract_temporary() as dumps_path:
                ...

If we don't want to extract all files, we can work directly with :class:`zipfile.ZipFile` object, which will be internally downloaded from S3 to the temporary file using :py:meth:`RemoteResource.download_temporary_file` method.

.. code-block:: python

    class KartonService(Karton):
        ...
        def process(self, task: Task) -> None:
            dumps = task.get_resource("dumps")

            with dumps.zip_file() as zipf:
                with zipf.open("sample_info.txt") as info:
                    ...

More information about resources can be found in API documentation.

Persistent payload
------------------

Part of payload that is propagated to the whole task subtree. The common use-case is to keep information related not with single artifact but the whole analysis, so they're available everywhere even if not explicitly passed by the Karton Service.

.. code-block:: python

    task = Task(
        headers=...,
        payload=...,
        payload_persistent={
            "uploader": "psrok1"
        }
    )

Incoming persistent payload (task received by Karton Service) is merged by Karton library with the outgoing tasks (result tasks sent by Karton Service). Karton service can't overwrite or delete the incoming payload keys. 

.. code-block:: python

    class KartonService(Karton):
        ...
        def process(self, task: Task) -> None:
            uploader = task.get_payload("uploader")

            assert task.is_payload_persistent("uploader")

            task = Task(
                headers=...,
                payload=...
            )
            # Outgoing task also contains "uploader" key
            self.send_task(task)

Regular payloads and persistent payload keys have common namespace so persistent payload can't be overwritten by regular payload as well e.g.

.. code-block:: python

    task = Task(
        headers=...,
        payload={
            "common_key": "<this will be ignored>"
        },
        payload_persistent={
            "common_key": "<and this value will be used>"
        }
    )

.. warning::

    Because merging strategy is quite aggressive, it's not recommended to overuse that feature. They should be treated as "analysis-wide payload". It's recommended to set them only in initial task.
    
    Don't store any references to resources or other heavy objects here, unless you need to. Persistent payload is, as the name says, persistent, so it is propagated to the whole task subtree and **can't be removed** during analysis. Resource referenced by persistent payload won't be garbage-collected until the whole analysis (task subtree) ends, even if it's not needed by further analysis steps.
