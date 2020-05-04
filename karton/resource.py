import contextlib
import os
import shutil
import sys
import tempfile
import uuid
import zipfile

from io import BytesIO


class ResourceBase(object):
    """
    Abstract base class for Resource objects.
    """

    def __init__(
        self,
        name,
        content=None,
        path=None,
        bucket=None,
        metadata=None,
        _uid=None,
        _size=None,
    ):
        self.name = name
        self.bucket = bucket
        self.metadata = metadata or {}

        if content and path:
            raise ValueError("Can't set both path and content for resource")
        if path:
            if not os.path.isfile(path):
                raise IOError("Path {path} doesn't exist or is not a file"
                              .format(path=path))
        elif content:
            if type(content) is str and sys.version_info >= (3, 0):
                content = content.encode()
            elif type(content) is not bytes:
                raise TypeError("Content can be bytes or str only")

        # Empty Resource is possible here (e.g. DirectoryResource doesn't have immediate content)

        self._uid = _uid or str(uuid.uuid4())
        self._content = content
        self._path = path
        self._size = _size

    @property
    def uid(self):
        """
        Resource identifier (UUID)

        :rtype: str
        """
        return self._uid

    @property
    def content(self):
        """
        Resource content

        :rtype: Optional[bytes]
        """
        return self._content

    @property
    def size(self):
        """
        Resource size

        :rtype: int
        """
        if self._size is None:
            if self._path:
                self._size = os.path.getsize(self._path)
            elif self._content:
                self._size = len(self._content)
        return self._size

    def to_dict(self):
        # Internal serialization method
        return {
            "uid": self.uid,
            "name": self.name,
            "bucket": self.bucket,
            "size": self.size,
            "metadata": self.metadata,
            "flags": [],
            "sha256": self.metadata.get("sha256")
        }


class LocalResource(ResourceBase):
    """
    Represents local resource with arbitrary binary data e.g. file contents.

    Local resources will be uploaded to object hub (Minio) during
    task dispatching.

    .. code-block:: python

        # Creating resource from bytes
        sample = Resource("original_name.exe", content=b"X5O!P%@AP[4
        \\PZX54(P^)7CC)7}$EICAR-STANDARD-ANT...")

        # Creating resource from path
        sample = Resource("original_name.exe", path="sample/original_name.exe")

    :param name: Name of the resource (e.g. name of file)
    :type name: str
    :param content: Resource content
    :type content: bytes or str
    :param path: Path of file with resource content
    :type path: str
    :param bucket: Alternative Minio bucket for resource
    :type bucket: str, optional
    :param metadata: Resource metadata
    :type metadata: dict, optional
    """

    def __init__(self, name, content=None, path=None, bucket=None, metadata=None):
        super(LocalResource, self).__init__(
            name, content=content, path=path, bucket=bucket, metadata=metadata
        )

    def _upload(self, minio):
        # Note: never transform resource into Remote (multiple task dispatching with same local,
        # in that case resource can be deleted between tasks)
        if self._content:
            # Upload contents
            minio.put_object(self.bucket, self.uid, BytesIO(self._content), len(self._content))
        else:
            # Upload file provided by path
            minio.fput_object(self.bucket, self.uid, self._path)

    def upload(self, minio):
        # Internal local resource upload method
        if not self._content and not self._path:
            raise RuntimeError("Can't upload resource without content")
        return self._upload(minio)


Resource = LocalResource


class RemoteResource(ResourceBase):
    """
    Keeps reference to remote resource object shared between subsystems via object hub (Minio)

    Should never be instantiated directly by subsystem, but can be directly passed to outgoing payload.
    """

    def __init__(
        self, name, bucket=None, metadata=None, uid=None, size=None, minio=None
    ):
        super(RemoteResource, self).__init__(
            name, bucket=bucket, metadata=metadata, _uid=uid, _size=size
        )
        self._minio = minio

    def loaded(self):
        """
        Checks whether resource is loaded into memory

        :rtype: bool
        """
        return self._content is not None

    @classmethod
    def from_dict(cls, dict, minio):
        """
        Internal deserialization method for remote resources

        :param dict: Serialized information about resource
        :type dict: Dict[str, Any]
        :param minio: Minio binding object
        :type minio: :class:`minio.Minio`

        :meta private:
        """
        # Backwards compatibility
        metadata = dict.get("metadata", {})
        if "sha256" in dict:
            metadata["sha256"] = dict["sha256"]

        return cls(
            name=dict["name"],
            metadata=metadata,
            bucket=dict["bucket"],
            uid=dict["uid"],
            size=dict.get("size"),  # Backwards compatibility
            minio=minio,
        )

    @property
    def content(self):
        """
        Resource content. Performs download when resource was not loaded before.

        Returns None if resource is local and payload is provided by path.
        :rtype: Optional[bytes]
        """
        if self._content is None:
            return self.download()
        return self._content

    def unload(self):
        """
        Unloads resource object from memory
        """
        self._content = None

    def remove(self):
        """
        Internal remote resource remove method

        :meta private:
        """
        self._minio.remove_object(self.bucket, self.uid)

    def download(self):
        """
        Downloads remote resource content from object hub into memory.

        .. code-block:: python

            sample = self.current_task.get_resource("sample")

            # Ensure that resource will be downloaded before it will be
            # passed to processing method
            sample.download()

            self.process_sample(sample)

        :rtype: bytes
        """
        reader = self._minio.get_object(self.bucket, self.uid)
        sio = BytesIO(reader.data)
        self._content = sio.getvalue()
        return self._content

    def download_to_file(self, path):
        """
        Downloads remote resource into file.

        .. code-block:: python

            sample = self.current_task.get_resource("sample")

            sample.download_to_file("sample/sample.exe")

            with open("sample/sample.exe", "rb") as f:
                contents = f.read()
        """
        self._minio.fget_object(self.bucket, self.uid, path)

    @contextlib.contextmanager
    def download_temporary_file(self):
        """
        Downloads remote resource into named temporary file.

        .. code-block:: python

            sample = self.current_task.get_resource("sample")

            with sample.download_temporary_file() as f:
                contents = f.read()
                path = f.name

            # Temporary file is deleted after exitting the "with" scope

        :rtype: ContextManager[BinaryIO]
        """
        # That tempfile-fu is necessary because minio.fget_object removes file under provided path and
        # and renames its own part-file with downloaded content under previously deleted path
        # Weird move, but ok...
        tmp = tempfile.NamedTemporaryFile(delete=False)
        tmp.close()
        try:
            self.download_to_file(tmp.name)
            with open(tmp.name, "rb") as f:
                yield f
        finally:
            os.remove(tmp.name)


class DirectoryResourceBase(ResourceBase):
    """
    Abstract base class for DirectoryResource objects.
    """

    DIRECTORY_FLAG = "Directory"

    def to_dict(self):
        data = super(DirectoryResourceBase, self).to_dict()
        data["flags"] += [DirectoryResourceBase.DIRECTORY_FLAG]
        return data


class LocalDirectoryResource(DirectoryResourceBase, LocalResource):
    """
    Resource extension, allowing to pass whole directory as a zipped resource.

    Directories tend to be large in size so it's preferred to operate on temporary file storage.
    Although if you know that your directories won't be that big: in-memory methods are still
    supported.

    .. code-block:: python

        # Creating directory resource from path
        dumps = DirectoryResource("dumps", directory_path="dumps/")

    :param name: Name of the resource (e.g. name of directory)
    :type name: str
    :param directory_path: Path of the resource directory
    :type directory_path: str
    :param compression: Compression level (default is zipfile.ZIP_DEFLATED)
    :type compression: int, optional
    :param bucket: Alternative Minio bucket for resource
    :type bucket: str, optional
    :param metadata: Resource metadata
    :type metadata: dict, optional
    """

    def __init__(
        self,
        name,
        directory_path,
        compression=zipfile.ZIP_DEFLATED,
        bucket=None,
        metadata=None,
    ):
        super(LocalDirectoryResource, self).__init__(
            name=name, bucket=bucket, metadata=metadata
        )
        self._directory_path = directory_path
        self._compression = compression

    def _make_zip(self, out_stream):
        # Recursively zips all files in directory_path keeping relative paths
        # File is zipped into provided out_stream
        with zipfile.ZipFile(out_stream, "w", compression=self._compression) as zipf:
            for root, dirs, files in os.walk(self._directory_path):
                for name in files:
                    abs_path = os.path.join(root, name)
                    zipf.write(
                        abs_path, os.path.relpath(abs_path, self._directory_path)
                    )

    def make_zip(self):
        """
        Prepares in-memory zip. Useful if you don't want to use temporary file storage
        during zip preparation for upload.

        Raw zip contents are available via :py:attr:`LocalDirectoryResource.content` property

        .. code-block:: python

            dumps = DirectoryResource("dumps", directory_path="dumps/")

            # Create in-memory zip
            dumps.make_zip()

            zipf = zipfile.ZipFile(BytesIO(dumps.content))
            print("Fetched dumps: ", zipf.namelist())
        """
        result_stream = BytesIO()
        self._make_zip(result_stream)
        self._content = result_stream.getvalue()

    @contextlib.contextmanager
    def _prepare_zip(self):
        # Prepares zip file to upload
        if self._content is not None:
            # If zip contents are available in memory: let's use them!
            yield
        else:
            try:
                # If not: let's create temporary file with zipped directory
                with tempfile.NamedTemporaryFile() as f:
                    # Create zipfile
                    self._make_zip(f)
                    # Flush contents
                    f.flush()
                    # Set file path
                    self._path = f.name
                    yield
            finally:
                # Clean-up (just to be tidy)
                self._path = None

    def upload(self, minio):
        """
        Uploads local resource object to minio.

        :param minio: Minio binding
        :type minio: :class:`minio.Minio`

        :meta private:
        """
        with self._prepare_zip():
            self._upload(minio)


DirectoryResource = LocalDirectoryResource


class RemoteDirectoryResource(DirectoryResourceBase, RemoteResource):
    """
    Keeps reference to remote directory resource object shared between subsystems via object hub (Minio)

    Inherits from RemoteResource. Contents of this resource are raw ZIP file data.

    Should never be instantiated directly by subsystem, but can be directly passed to outgoing payload.
    """

    @contextlib.contextmanager
    def zip_file(self):
        """
        Allows to operate on ZipFile object.

        .. code-block:: python

            dumps = self.current_task.get_resource("dumps")

            with dumps.zip_file() as zipf:
                print("Fetched dumps: ", zipf.namelist())

        By default: method downloads zip into temporary file, which is deleted after
        leaving the context. If you want to load zip into memory,
        call :py:meth:`RemoteResource.download` first.

        If you want to pre-download Zip under specified path and open it using zipfile module,
        you need to do this manually:

        .. code-block:: python

            dumps = self.current_task.get_resource("dumps")

            # Download zip file
            zip_path = "./dumps.zip"
            dumps.download_to_file(zip_path)

            zipf = zipfile.Zipfile(zip_path)

        :rtype: ContextManager[zipfile.ZipFile]
        """
        if self._content:
            yield zipfile.ZipFile(BytesIO(self._content))
        else:
            with self.download_temporary_file() as f:
                yield zipfile.ZipFile(f)

    def extract_to_directory(self, path):
        """
        Extracts files contained in RemoteDirectoryResource into provided path.

        By default: method downloads zip into temporary file, which is deleted after
        extraction. If you want to load zip into memory, call :py:meth:`RemoteDirectoryResource.download`
        first.
        """
        with self.zip_file() as zf:
            zf.extractall(path)

    @contextlib.contextmanager
    def extract_temporary(self):
        """
        Extracts files contained in RemoteDirectoryResource to temporary directory.

        Returns path of directory with extracted files. Directory is recursively deleted after
        leaving the context.

        .. code-block:: python

            dumps = self.current_task.get_resource("dumps")

            with dumps.extract_temporary() as dumps_path:
                print("Fetched dumps:", os.listdir(dumps_path))

        By default: method downloads zip into temporary file, which is deleted after
        extraction. If you want to load zip into memory, call :py:meth:`RemoteDirectoryResource.download`
        first.

        :rtype: ContextManager[str]
        """
        tmpdir = tempfile.mkdtemp()
        try:
            self.extract_to_directory(tmpdir)
            yield tmpdir
        finally:
            shutil.rmtree(tmpdir)


def remote_resource_from_dict(dict, minio):
    # Internal deserialization method
    if RemoteDirectoryResource.DIRECTORY_FLAG in dict.get("flags", []):
        return RemoteDirectoryResource.from_dict(dict, minio)
    else:
        return RemoteResource.from_dict(dict, minio)
