<!-- karton documentation master file, created by
sphinx-quickstart on Fri Mar 22 17:40:08 2019.
You can adapt this file completely to your liking, but it should at least
contain the root `toctree` directive. -->
# Welcome to karton’s documentation!


#### class karton.Producer(config, \*\*kwargs)

#### send_task(task)
Sends a task to the RabbitMQ queue. Takes care of logging.
Given task will be child of task we are currently handling (if such exists) - this ensures our log continuity
:param task: `karton.Task` to be sent
:return: bool if task was delivered


#### class karton.Consumer(config)
Base consumer class, expected to be inherited from


#### download_resource(resource)
Download remote resource into local resource.


* **Parameters**

    **resource** – `karton.RemoteResource` to download



* **Returns**

    `karton.Resource`



#### download_to_temporary_folder(resource)
Context manager for downloading remote directory resource into local temporary folder.
It also makes sure that the temporary folder is disposed afterwards.


* **Parameters**

    **resource** – `karton.RemoteDirectoryResource`



* **Returns**

    path to temporary folder with unpacked contents



#### download_zip_file(resource)
Download remote directory resource contents into Zipfile object.


* **Parameters**

    **resource** – `karton.RemoteDirectoryResource`



* **Returns**

    `zipfile.Zipfile`



#### loop()
Blocking loop that consumes tasks and runs `karton.Consumer.process()` as a handler


#### process()
Expected to be overwritten

self.current_task contains task that triggered invokation of `karton.Consumer.process()`


#### remove_resource(resource)
Remove remote resource.
:param: `karton.RemoteResource` to be removes


#### upload_resource(resource)
Upload local resource to the storage hub


* **Parameters**

    **resource** – `karton.Resource` to upload



* **Returns**

    `karton.RemoteResource` representing uploaded resource



#### class karton.Karton(config)
This glues together Consumer and Producer - which is the most common use case


#### class karton.Resource(name, content, size=None, _uid=None)

#### download(minio)
Download RemoteResource into object for local usage
:param minio: minio instance
:return: `karton.Resource` - local resource


#### get_size(minio)
Gets size of remote object (without downloading content)
:param minio: minio instance
:return: size of remote object


#### remove(minio)
Remove remote resource from minio storage
:param minio: minio instance


#### upload(minio, bucket)
This is where we sync with remote, never to be used by user explicitly
Should be invoked while uploading task
:return: RemoteResource to use locally


#### class karton.DirectoryResource(name, bucket, directory_path, \*args, \*\*kwargs)

#### class karton.RemoteResource(name, bucket=None, _uid=None)
Abstraction over remote minio objects.

This exists to make it easier to share resources across clients

Resources are independent of underlying minio objects for easier local manipulation


#### download(minio)
Download RemoteResource into object for local usage
:param minio: minio instance
:return: `karton.Resource` - local resource


#### get_size(minio)
Gets size of remote object (without downloading content)
:param minio: minio instance
:return: size of remote object


#### is_directory()
Helps to identify DirectoryResource vs Resource without type checking
:return: true if this instance is RemoteDirectoryResource or DirectoryResource


#### remove(minio)
Remove remote resource from minio storage
:param minio: minio instance


#### class karton.Task(headers, payload=None)

#### add_payload(name, content)
Add payload to task
:param name: name of the payload
:param resource: payload to be added


#### add_resource(name, resource)
Add resource to task
:param name: name of the resource
:param resource: `karton.Resource` - resource to be added


#### classmethod derive_task(headers, task)
Alternative constructor which copies payload from given task, useful for proxying resource with added metadata.
:param headers: same as in normal constructor
:param task: `karton.Task` - task to derive from
:return: `karton.Task` with new headers


#### get_directory_resources()

* **Returns**

    Generator of all directory resources present in the `karton.PayloadBag`



#### get_file_resources()

* **Returns**

    Generator of all file resources present in the `karton.PayloadBag`



#### get_payload(name, default=None)
Get payload from task
:param name: name of the payload
:param default: value to be returned if payload is not present
:return: payload content


#### get_resource(name, default=None)
Get resource from task
:param name: name of the resource
:param default: value to be returned if resource is not present
:return: `karton.Resource` - resource with given name


#### get_resources()

* **Returns**

    Generator of all resources present in the `karton.PayloadBag`



#### is_asynchronic()

* **Returns**

    bool - If current task is asynchronic



#### make_asynchronic()
Task declares that work will be done by some remote
handler, so task shouldn’t be considered finished when process() returns

Useful for processing long running tasks - eg. in cuckoo we finish task only after analysis


#### payload_contains(name)

* **Parameters**

    **name** – name of the payload to be checked



* **Returns**

    bool - if task’s payload contains payload with given name



#### remove_payload(name)
Removes payload for the task
:param name: payload name to be removed


#### set_task_parent(parent)
Bind existing Task to parent task
:param parent: `karton.Task` - task to bind to

# Indices and tables

* Index

* Module Index

* Search Page
