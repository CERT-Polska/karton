import contextlib
import os
import shutil
import sys
import tempfile
import uuid
import zipfile
import hashlib

from io import BytesIO


class ResourceBase(object):
    """
    Abstract base class for Resource objects.
    """
    DIRECTORY_FLAG = "Directory"

    def __init__(
        self,
        name,
        content=None,
        path=None,
        bucket=None,
        metadata=None,
        sha256=None,
        _uid=None,
        _size=None,
        _flags=None
    ):
        self.name = name
        self.bucket = bucket
        self.metadata = metadata or {}
        # the sha256 identifier can be passed as an argument or inside the metadata
        sha256 = sha256 or self.metadata.get("sha256")

        calculate_hash = sha256 is None

        if content and path:
            raise ValueError("Can't set both path and content for resource")
        if path:
            if not os.path.isfile(path):
                raise IOError("Path {path} doesn't exist or is not a file"
                              .format(path=path))
            if calculate_hash:
                sha256_hash = hashlib.sha256()
                with open(path, "rb") as f:
                    for byte_block in iter(lambda: f.read(4096), b""):
                        sha256_hash.update(byte_block)
                sha256 = sha256_hash.hexdigest()
        elif content:
            if type(content) is str and sys.version_info >= (3, 0):
                content = content.encode()
            elif type(content) is not bytes:
                raise TypeError("Content can be bytes or str only")
            if calculate_hash:
                sha256 = hashlib.sha256(content).hexdigest()

        # Empty Resource is possible here (e.g. RemoteResource)

        # All normal Resources have to have a sha256 value that identifies them
        if sha256 is None:
            raise ValueError("Trying to create a new resource without known sha256 identifier")

        self.metadata["sha256"] = sha256

        self._uid = _uid or str(uuid.uuid4())
        self._content = content
        self._path = path
        self._size = _size
        # Flags needed by 3.x.x Karton services
        self._flags = _flags or []

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

    @property
    def sha256(self):
        """
        Resource sha256

        :rtype: str
        """
        sha256 = self.metadata.get("sha256")
        if sha256 is None:
            # SHA256 can be missing in resources from older Karton versions
            raise ValueError("Resource is missing sha256")
        return sha256

    def to_dict(self):
        # Internal serialization method
        return {
            "uid": self.uid,
            "name": self.name,
            "bucket": self.bucket,
            "size": self.size,
            "metadata": self.metadata,
            "flags": self._flags,
            "sha256": self.sha256
        }


class LocalResource(ResourceBase):
    """
    Represents local resource with arbitrary binary data e.g. file contents.

    Local resources will be uploaded to object hub (MinIO) during
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
    :param bucket: Alternative MinIO bucket for resource
    :type bucket: str, optional
    :param metadata: Resource metadata
    :type metadata: dict, optional
    :param uid: Alternative MinIO resource id
    :type uid: str, optional
    :param uid: Resource sha256 hash
    :type uid: str, optional
    """
    def __init__(self, name, content=None, path=None, bucket=None, metadata=None, uid=None, sha256=None,
                 _fd=None, _flags=None):
        super(LocalResource, self).__init__(
            name, content=content, path=path, bucket=bucket, metadata=metadata, sha256=sha256, _uid=uid, _flags=_flags
        )
        self._fd = _fd

    @classmethod
    def from_directory(
        cls,
        name,
        directory_path,
        compression=zipfile.ZIP_DEFLATED,
        in_memory=False,
        bucket=None,
        metadata=None,
        uid=None
    ):
        """
        Resource extension, allowing to pass whole directory as a zipped resource.

        Reads all files contained in directory_path recursively and packs them into zip file.

        .. code-block:: python

            # Creating zipped resource from path
            dumps = LocalResource.from_directory("dumps", directory_path="dumps/")

        :param name: Name of the resource (e.g. name of file)
        :type name: str
        :param directory_path: Path of the resource directory
        :type directory_path: str
        :param compression: Compression level (default is zipfile.ZIP_DEFLATED)
        :type compression: int, optional
        :param in_memory: Don't create temporary file and make in-memory zip file (default: False)
        :type in_memory: bool, optional
        :param bucket: Alternative MinIO bucket for resource
        :type bucket: str, optional
        :param metadata: Resource metadata
        :type metadata: dict, optional
        :param uid: Alternative MinIO resource id
        :type uid: str, optional
        :return: :class:`LocalResource` instance with zipped contents
        """
        out_stream = BytesIO() if in_memory else tempfile.NamedTemporaryFile()

        # Recursively zips all files in directory_path keeping relative paths
        # File is zipped into provided out_stream
        with zipfile.ZipFile(out_stream, "w", compression=compression) as zipf:
            for root, dirs, files in os.walk(directory_path):
                for name in files:
                    abs_path = os.path.join(root, name)
                    zipf.write(
                        abs_path, os.path.relpath(abs_path, directory_path)
                    )

        # Flag is required by Karton 3.x.x services to recognize that resource as DirectoryResource
        flags = [ResourceBase.DIRECTORY_FLAG]

        if in_memory:
            return cls(name, content=out_stream.getvalue(), bucket=bucket, metadata=metadata,
                       uid=uid, _flags=flags)
        else:
            return cls(name, path=out_stream.name, bucket=bucket, metadata=metadata, uid=uid,
                       _fd=out_stream, _flags=flags)

    def _upload(self, minio):
        # Note: never transform resource into Remote (multiple task dispatching with same local,
        # in that case resource can be deleted between tasks)
        if self._content:
            # Upload contents
            minio.put_object(self.bucket, self.uid, BytesIO(self._content), len(self._content))
        else:
            # Upload file provided by path
            minio.fput_object(self.bucket, self.uid, self._path)
        # If file descriptor is managed by Resource, close it after upload
        if self._fd:
            self._fd.close()

    def upload(self, minio):
        # Internal local resource upload method
        if not self._content and not self._path:
            raise RuntimeError("Can't upload resource without content")
        return self._upload(minio)


Resource = LocalResource


class RemoteResource(ResourceBase):
    """
    Keeps reference to remote resource object shared between subsystems via object hub (MinIO)

    Should never be instantiated directly by subsystem, but can be directly passed to outgoing payload.
    """

    def __init__(
        self, name, bucket=None, metadata=None, uid=None, size=None, minio=None, sha256=None, _flags=None
    ):
        super(RemoteResource, self).__init__(
            name, bucket=bucket, metadata=metadata, sha256=sha256, _uid=uid, _size=size, _flags=_flags
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
            size=dict.get("size"),  # Backwards compatibility (2.x.x)
            minio=minio,
            _flags=dict.get("flags")  # Backwards compatibility (3.x.x)
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

    @contextlib.contextmanager
    def zip_file(self):
        """
        If resource contains a Zip file, downloads it to the temporary file
        and wraps it with ZipFile object.

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
        If resource contains a Zip file, extracts files contained in Zip into provided path.

        By default: method downloads zip into temporary file, which is deleted after
        extraction. If you want to load zip into memory, call :py:meth:`RemoteResource.download`
        first.
        """
        with self.zip_file() as zf:
            zf.extractall(path)

    @contextlib.contextmanager
    def extract_temporary(self):
        """
        If resource contains a Zip file, extracts files contained in Zip to the temporary directory.

        Returns path of directory with extracted files. Directory is recursively deleted after
        leaving the context.

        .. code-block:: python

            dumps = self.current_task.get_resource("dumps")

            with dumps.extract_temporary() as dumps_path:
                print("Fetched dumps:", os.listdir(dumps_path))

        By default: method downloads zip into temporary file, which is deleted after
        extraction. If you want to load zip into memory, call :py:meth:`RemoteResource.download`
        first.

        :rtype: ContextManager[str]
        """
        tmpdir = tempfile.mkdtemp()
        try:
            self.extract_to_directory(tmpdir)
            yield tmpdir
        finally:
            shutil.rmtree(tmpdir)
