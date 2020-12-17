import contextlib
import hashlib
import os
import shutil
import tempfile
import uuid
import zipfile
from io import BytesIO
from typing import (
    TYPE_CHECKING,
    Any,
    BinaryIO,
    Dict,
    Iterator,
    List,
    Optional,
    Union,
    cast,
)

if TYPE_CHECKING:
    from .backend import KartonBackend


class ResourceBase(object):
    """
    Base resource class, contains the basic logic of both remote and
    local resources. If you're not implementing your own resource metatypes
    you'll probably want to look at `:py:meth:`LocalResource or
    `:py:meth:` RemoteResource instead.

    :param name: Name of the resource (e.g. name of file)
    :param content: Resource content
    :param path: Path of file with resource content
    :param bucket: Alternative MinIO bucket for resource
    :param metadata: Resource metadata
    :param sha256: Resource sha256 hash
    :param _uid: Alternative MinIO resource id
    :param _fd: File descriptor
    :param _flag: Resource flags
    """

    DIRECTORY_FLAG = "Directory"

    def __init__(
        self,
        name: str,
        content: Optional[Union[str, bytes]] = None,
        path: Optional[str] = None,
        bucket: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        sha256: Optional[str] = None,
        _uid: Optional[str] = None,
        _size: Optional[int] = None,
        _flags: Optional[List[str]] = None,
    ) -> None:
        self.name = name
        self.bucket = bucket
        self.metadata = metadata or {}
        # the sha256 identifier can be passed as an argument or inside the metadata
        sha256 = sha256 or self.metadata.get("sha256")

        calculate_hash = sha256 is None

        self._content: Optional[bytes] = None

        if content and path:
            raise ValueError("Can't set both path and content for resource")
        if path:
            if not os.path.isfile(path):
                raise IOError(
                    "Path {path} doesn't exist or is not a file".format(path=path)
                )
            if calculate_hash:
                sha256_hash = hashlib.sha256()
                with open(path, "rb") as f:
                    for byte_block in iter(lambda: f.read(4096), b""):
                        sha256_hash.update(byte_block)
                sha256 = sha256_hash.hexdigest()
        elif content:
            if isinstance(content, str):
                self._content = content.encode()
            elif isinstance(content, bytes):
                self._content = content
            else:
                raise TypeError("Content can be bytes or str only")
            if calculate_hash and self._content:
                sha256 = hashlib.sha256(self._content).hexdigest()

        # Empty Resource is possible here (e.g. RemoteResource)

        # All normal Resources have to have a sha256 value that identifies them
        if sha256 is None:
            raise ValueError(
                "Trying to create a new resource without known sha256 identifier"
            )

        self.metadata["sha256"] = sha256

        self._uid = _uid or str(uuid.uuid4())
        self._path = path
        self._size = _size
        # Flags needed by 3.x.x Karton services
        self._flags = _flags or []

    @property
    def uid(self) -> str:
        """
        Resource identifier (UUID)

        :return: Resource identifier
        """
        return self._uid

    @property
    def content(self) -> Optional[bytes]:
        """
        Resource content

        :return: Resource contents
        """
        return self._content

    @property
    def size(self) -> int:
        """
        Resource size

        :return: Resource size
        """
        if self._size is None:
            if self._path:
                self._size = os.path.getsize(self._path)
            elif self._content:
                self._size = len(self._content)
        return cast(int, self._size)

    @property
    def sha256(self) -> str:
        """
        Resource sha256

        :return: Hexencoded resource SHA256 hash
        """
        sha256 = self.metadata.get("sha256")
        if sha256 is None:
            # SHA256 can be missing in resources from older Karton versions
            raise ValueError("Resource is missing sha256")
        return sha256

    def to_dict(self) -> Dict[str, Any]:
        # Internal serialization method
        return {
            "uid": self.uid,
            "name": self.name,
            "bucket": self.bucket,
            "size": self.size,
            "metadata": self.metadata,
            "flags": self._flags,
            "sha256": self.sha256,
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
    :param content: Resource content
    :param path: Path of file with resource content
    :param bucket: Alternative MinIO bucket for resource
    :param metadata: Resource metadata
    :param uid: Alternative MinIO resource id
    :param sha256: Resource sha256 hash
    :param _fd: File descriptor
    :param _flag: Resource flags
    """

    def __init__(
        self,
        name: str,
        content: Optional[bytes] = None,
        path: Optional[str] = None,
        bucket: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        uid: Optional[str] = None,
        sha256: Optional[str] = None,
        _fd: Optional[BinaryIO] = None,
        _flags: Optional[List[str]] = None,
    ) -> None:
        super(LocalResource, self).__init__(
            name,
            content=content,
            path=path,
            bucket=bucket,
            metadata=metadata,
            sha256=sha256,
            _uid=uid,
            _flags=_flags,
        )
        self._fd = _fd

    @classmethod
    def from_directory(
        cls,
        name: str,
        directory_path: str,
        compression: int = zipfile.ZIP_DEFLATED,
        in_memory: bool = False,
        bucket: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        uid: Optional[str] = None,
    ) -> "LocalResource":
        """
        Resource extension, allowing to pass whole directory as a zipped resource.

        Reads all files contained in directory_path recursively and packs them
        into zip file.

        .. code-block:: python

            # Creating zipped resource from path
            dumps = LocalResource.from_directory("dumps", directory_path="dumps/")

        :param name: Name of the resource (e.g. name of file)
        :param directory_path: Path of the resource directory
        :param compression: Compression level (default is zipfile.ZIP_DEFLATED)
        :param in_memory: Don't create temporary file and make in-memory zip file \
                          (default: False)
        :param bucket: Alternative MinIO bucket for resource
        :param metadata: Resource metadata
        :param uid: Alternative MinIO resource id
        :return: :class:`LocalResource` instance with zipped contents
        """
        out_stream = BytesIO() if in_memory else tempfile.NamedTemporaryFile()

        # Recursively zips all files in directory_path keeping relative paths
        # File is zipped into provided out_stream
        with zipfile.ZipFile(out_stream, "w", compression=compression) as zipf:
            for root, dirs, files in os.walk(directory_path):
                for name in files:
                    abs_path = os.path.join(root, name)
                    zipf.write(abs_path, os.path.relpath(abs_path, directory_path))

        # Flag is required by Karton 3.x.x services to recognize that resource
        # as DirectoryResource
        flags = [ResourceBase.DIRECTORY_FLAG]

        if in_memory:
            return cls(
                name,
                content=cast(BytesIO, out_stream).getvalue(),
                bucket=bucket,
                metadata=metadata,
                uid=uid,
                _flags=flags,
            )
        else:
            return cls(
                name,
                path=out_stream.name,
                bucket=bucket,
                metadata=metadata,
                uid=uid,
                _fd=cast(BinaryIO, out_stream),
                _flags=flags,
            )

    def _upload(self, backend: "KartonBackend") -> None:
        """Internal function for uploading resources

        :param backend: KartonBackend to use while uploading the resource

        :meta private:
        """

        # Note: never transform resource into Remote
        # Multiple task dispatching with same local, in that case resource
        # can be deleted between tasks.
        if self.bucket is None:
            raise RuntimeError(
                "Resource object can't be uploaded because its bucket is not set"
            )

        if self._content:
            # Upload contents
            backend.upload_object(self.bucket, self.uid, self._content)
        else:
            # Upload file provided by path
            backend.upload_object_from_file(
                self.bucket, self.uid, cast(str, self._path)
            )
        # If file descriptor is managed by Resource, close it after upload
        if self._fd:
            self._fd.close()

    def upload(self, backend: "KartonBackend") -> None:
        """Internal function for uploading resources

        :param backend: KartonBackend to use while uploading the resource

        :meta private:
        """
        if not self._content and not self._path:
            raise RuntimeError("Can't upload resource without content")
        self._upload(backend)


Resource = LocalResource


class RemoteResource(ResourceBase):
    """
    Keeps reference to remote resource object shared between subsystems
    via object storage (MinIO)

    Should never be instantiated directly by subsystem, but can be directly passed to
    outgoing payload.

    :param name: Name of the resource (e.g. name of file)
    :param bucket: Alternative MinIO bucket for resource
    :param metadata: Resource metadata
    :param uid: Alternative MinIO resource id
    :param size: Resource size
    :param backend: :py:meth:`KartonBackend` to bind to this resource
    :param sha256: Resource sha256 hash
    :param _flag: Resource flags
    """

    def __init__(
        self,
        name: str,
        bucket: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        uid: Optional[str] = None,
        size: Optional[int] = None,
        backend: Optional["KartonBackend"] = None,
        sha256: Optional[str] = None,
        _flags: Optional[List[str]] = None,
    ) -> None:
        super(RemoteResource, self).__init__(
            name,
            bucket=bucket,
            metadata=metadata,
            sha256=sha256,
            _uid=uid,
            _size=size,
            _flags=_flags,
        )
        self.backend = backend

    def loaded(self) -> bool:
        """
        Checks whether resource is loaded into memory

        :return: Flag indicating if the resource is loaded or not
        """
        return self._content is not None

    @classmethod
    def from_dict(
        cls, dict: Dict[str, Any], backend: Optional["KartonBackend"]
    ) -> "RemoteResource":
        """
        Internal deserialization method for remote resources

        :param dict: Serialized information about resource
        :param backend: KartonBackend object
        :return: Deserialized :py:meth:`RemoteResource` object

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
            backend=backend,
            _flags=dict.get("flags"),  # Backwards compatibility (3.x.x)
        )

    @property
    def content(self) -> Optional[bytes]:
        """
        Resource content. Performs download when resource was not loaded before.

        Returns None if resource is local and payload is provided by path.
        :return: Content bytes
        """
        if self._content is None:
            return self.download()
        return self._content

    def unload(self) -> None:
        """
        Unloads resource object from memory
        """
        self._content = None

    def remove(self) -> None:
        """
        Internal remote resource remove method

        :meta private:
        """
        if self.backend is None:
            raise RuntimeError(
                "Resource object can't be removed because it's not bound to the backend"
            )
        if self.bucket is None:
            raise RuntimeError(
                "Resource object can't be removed because its bucket is not set"
            )

        self.backend.remove_object(self.bucket, self.uid)

    def download(self) -> bytes:
        """
        Downloads remote resource content from object hub into memory.

        .. code-block:: python

            sample = self.current_task.get_resource("sample")

            # Ensure that resource will be downloaded before it will be
            # passed to processing method
            sample.download()

            self.process_sample(sample)

        :return: Downloaded content bytes
        """
        if self.backend is None:
            raise RuntimeError(
                (
                    "Resource object can't be downloaded because it's not bound to "
                    "the backend"
                )
            )
        if self.bucket is None:
            raise RuntimeError(
                "Resource object can't be downloaded because its bucket is not set"
            )

        self._content = self.backend.download_object(self.bucket, self.uid)
        return self._content

    def download_to_file(self, path: str) -> None:
        """
        Downloads remote resource into file.

        .. code-block:: python

            sample = self.current_task.get_resource("sample")

            sample.download_to_file("sample/sample.exe")

            with open("sample/sample.exe", "rb") as f:
                contents = f.read()

        :param path: Path to download the resource into
        """
        if self.backend is None:
            raise RuntimeError(
                (
                    "Resource object can't be downloaded because it's not bound to "
                    "the backend"
                )
            )
        if self.bucket is None:
            raise RuntimeError(
                "Resource object can't be downloaded because its bucket is not set"
            )

        self.backend.download_object_to_file(self.bucket, self.uid, path)

    @contextlib.contextmanager
    def download_temporary_file(self) -> Iterator[BinaryIO]:
        """
        Downloads remote resource into named temporary file.

        .. code-block:: python

            sample = self.current_task.get_resource("sample")

            with sample.download_temporary_file() as f:
                contents = f.read()
                path = f.name

            # Temporary file is deleted after exitting the "with" scope

        :return: ContextManager with the temporary file
        """
        # That tempfile-fu is necessary because minio.fget_object removes file
        # under provided path and renames its own part-file with downloaded content
        # under previously deleted path
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
    def zip_file(self) -> Iterator[zipfile.ZipFile]:
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

        If you want to pre-download Zip under specified path and open it using
        zipfile module, you need to do this manually:

        .. code-block:: python

            dumps = self.current_task.get_resource("dumps")

            # Download zip file
            zip_path = "./dumps.zip"
            dumps.download_to_file(zip_path)

            zipf = zipfile.Zipfile(zip_path)

        :return: ContextManager with zipfile
        """
        if self._content:
            yield zipfile.ZipFile(BytesIO(self._content))
        else:
            with self.download_temporary_file() as f:
                yield zipfile.ZipFile(f)

    def extract_to_directory(self, path: str) -> None:
        """
        If resource contains a Zip file, extracts files contained in Zip into
        provided path.

        By default: method downloads zip into temporary file, which is deleted
        after extraction. If you want to load zip into memory, call
        :py:meth:`RemoteResource.download` first.

        :param path: Directory path where the resource should be unpacked
        """
        with self.zip_file() as zf:
            zf.extractall(path)

    @contextlib.contextmanager
    def extract_temporary(self) -> Iterator[str]:
        """
        If resource contains a Zip file, extracts files contained in Zip
        to the temporary directory.

        Returns path of directory with extracted files. Directory is recursively
        deleted after leaving the context.

        .. code-block:: python

            dumps = self.current_task.get_resource("dumps")

            with dumps.extract_temporary() as dumps_path:
                print("Fetched dumps:", os.listdir(dumps_path))

        By default: method downloads zip into temporary file, which is deleted
        after extraction. If you want to load zip into memory, call
        :py:meth:`RemoteResource.download` first.

        :return: ContextManager with the temporary directory
        """
        tmpdir = tempfile.mkdtemp()
        try:
            self.extract_to_directory(tmpdir)
            yield tmpdir
        finally:
            shutil.rmtree(tmpdir)
