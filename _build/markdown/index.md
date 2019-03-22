<!-- karton documentation master file, created by
sphinx-quickstart on Fri Mar 22 17:40:08 2019.
You can adapt this file completely to your liking, but it should at least
contain the root `toctree` directive. -->
# Welcome to karton’s documentation!


#### class karton.Producer(config, \*\*kwargs)

#### class karton.Consumer(config)
Base consumer class, expected to be inherited from


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


#### class karton.DirectoryResource(name, bucket, directory_path, \*args, \*\*kwargs)

#### class karton.RemoteResource(name, bucket=None, _uid=None)
Abstraction over remote minio objects.

This exists to make it easier to share resources across clients

Resources are independent of underlying minio objects for easier local manipulation


#### class karton.Task(headers, payload=None)
# Indices and tables

* Index

* Module Index

* Search Page
