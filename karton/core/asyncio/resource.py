import contextlib
import os
import shutil
import tempfile
import zipfile
from io import BytesIO
from typing import IO, TYPE_CHECKING, Any, AsyncIterator, Dict, List, Optional, Union

from karton.core.resource import LocalResourceBase, ResourceBase

if TYPE_CHECKING:
    from .backend import KartonAsyncBackend


class LocalResource(LocalResourceBase):
    """
    Represents local resource with arbitrary binary data e.g. file contents.

    Local resources will be uploaded to object hub (S3) during
    task dispatching.

    .. code-block:: python

        # Creating resource from bytes
        sample = Resource("original_name.exe", content=b"X5O!P%@AP[4\\
        PZX54(P^)7CC)7}$EICAR-STANDARD-ANT...")

        # Creating resource from path
        sample = Resource("original_name.exe", path="sample/original_name.exe")

    :param name: Name of the resource (e.g. name of file)
    :param content: Resource content
    :param path: Path of file with resource content
    :param bucket: Alternative S3 bucket for resource
    :param metadata: Resource metadata
    :param uid: Alternative S3 resource id
    :param sha256: Resource sha256 hash
    :param fd: Seekable file descriptor
    :param _flags: Resource flags
    :param _close_fd: Close file descriptor after upload (default: False)
    """

    def __init__(
        self,
        name: str,
        content: Optional[Union[str, bytes]] = None,
        path: Optional[str] = None,
        bucket: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        uid: Optional[str] = None,
        sha256: Optional[str] = None,
        fd: Optional[IO[bytes]] = None,
        _flags: Optional[List[str]] = None,
        _close_fd: bool = False,
    ) -> None:
        super().__init__(
            name=name,
            content=content,
            path=path,
            bucket=bucket,
            metadata=metadata,
            uid=uid,
            sha256=sha256,
            fd=fd,
            _flags=_flags,
            _close_fd=_close_fd,
        )

    async def _upload(self, backend: "KartonAsyncBackend") -> None:
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
            await backend.upload_object(self.bucket, self.uid, self._content)
        elif self.fd:
            if self.fd.tell() != 0:
                raise RuntimeError(
                    f"Resource object can't be uploaded: "
                    f"file descriptor must point at first byte "
                    f"(fd.tell = {self.fd.tell()})"
                )
            # Upload contents from fd
            await backend.upload_object(self.bucket, self.uid, self.fd)
            # If file descriptor is managed by Resource, close it after upload
            if self._close_fd:
                self.fd.close()
        elif self._path:
            # Upload file provided by path
            await backend.upload_object_from_file(self.bucket, self.uid, self._path)

    async def upload(self, backend: "KartonAsyncBackend") -> None:
        """Internal function for uploading resources

        :param backend: KartonBackend to use while uploading the resource

        :meta private:
        """
        if not self._content and not self._path and not self.fd:
            raise RuntimeError("Can't upload resource without content")
        await self._upload(backend)


Resource = LocalResource


class RemoteResource(ResourceBase):
    """
    Keeps reference to remote resource object shared between subsystems
    via object storage (S3)

    Should never be instantiated directly by subsystem, but can be directly passed to
    outgoing payload.

    :param name: Name of the resource (e.g. name of file)
    :param bucket: Alternative S3 bucket for resource
    :param metadata: Resource metadata
    :param uid: Alternative S3 resource id
    :param size: Resource size
    :param backend: :py:meth:`KartonBackend` to bind to this resource
    :param sha256: Resource sha256 hash
    :param _flags: Resource flags
    """

    def __init__(
        self,
        name: str,
        bucket: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        uid: Optional[str] = None,
        size: Optional[int] = None,
        backend: Optional["KartonAsyncBackend"] = None,
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

    @property
    def content(self) -> bytes:
        """
        Resource content. Performs download when resource was not loaded before.

        :return: Content bytes
        """
        if self._content is None:
            raise RuntimeError(
                "Resource object needs to be explicitly downloaded first"
            )
        return self._content

    @classmethod
    def from_dict(
        cls, dict: Dict[str, Any], backend: Optional["KartonAsyncBackend"]
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

    def unload(self) -> None:
        """
        Unloads resource object from memory
        """
        self._content = None

    async def download(self) -> bytes:
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

        self._content = await self.backend.download_object(self.bucket, self.uid)
        return self._content

    async def download_to_file(self, path: str) -> None:
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

        await self.backend.download_object_to_file(self.bucket, self.uid, path)

    @contextlib.asynccontextmanager
    async def download_temporary_file(self, suffix=None) -> AsyncIterator[IO[bytes]]:
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
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
        tmp.close()
        try:
            await self.download_to_file(tmp.name)
            with open(tmp.name, "rb") as f:
                yield f
        finally:
            os.remove(tmp.name)

    @contextlib.asynccontextmanager
    async def zip_file(self) -> AsyncIterator[zipfile.ZipFile]:
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
            async with self.download_temporary_file() as f:
                yield zipfile.ZipFile(f)

    async def extract_to_directory(self, path: str) -> None:
        """
        If resource contains a Zip file, extracts files contained in Zip into
        provided path.

        By default: method downloads zip into temporary file, which is deleted
        after extraction. If you want to load zip into memory, call
        :py:meth:`RemoteResource.download` first.

        :param path: Directory path where the resource should be unpacked
        """
        async with self.zip_file() as zf:
            zf.extractall(path)

    @contextlib.asynccontextmanager
    async def extract_temporary(self) -> AsyncIterator[str]:
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
            await self.extract_to_directory(tmpdir)
            yield tmpdir
            yield tmpdir
        finally:
            shutil.rmtree(tmpdir)
