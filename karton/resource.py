import contextlib
import json
import shutil
import tempfile
import uuid
import zipfile
import sys
import hashlib
from io import BytesIO

from .utils import zip_dir


class NoContentException(Exception):
    pass


class ContentDoesNotExist(Exception):
    pass


class NotConfiguredResource(Exception):
    pass


class LocalResourceCanNotBeRemoved(Exception):
    pass


class LocalResourceCanNotBeDownloaded(Exception):
    pass


class ResourceFlagEnum(object):
    DIRECTORY = "Directory"


class RemoteResource(object):
    """
    Abstraction over remote minio objects.

    This exists to make it easier to share resources across clients

    Resources are independent of underlying minio objects for easier local manipulation
    """

    def __init__(self, name, bucket=None, _uid=None, sha256=None, flags=None):
        if _uid is None:
            _uid = str(uuid.uuid4())

        self.name = name
        self.bucket = bucket
        self.uid = _uid
        self.sha256 = sha256

        self.flags = flags or []

    def is_directory(self):
        """
        Helps to identify DirectoryResource vs Resource without type checking

        :rtype: bool
        :return: if this instance is derived from :py:class:`karton.RemoteDirectoryResource`
        """
        # both conditions should be identical
        return ResourceFlagEnum.DIRECTORY in self.flags or isinstance(
            self, RemoteDirectoryResource
        )

    def to_dict(self):
        return {
            "uid": self.uid,
            "name": self.name,
            "bucket": self.bucket,
            "flags": self.flags,
            "sha256": self.sha256,
        }

    def serialize(self):
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, data_dict):
        bucket = data_dict["bucket"]
        name = data_dict["name"]
        _uid = data_dict["uid"]
        sha256 = data_dict.get("sha256")
        flags = data_dict["flags"]

        new_cls = cls(name, bucket=bucket, _uid=_uid, sha256=sha256, flags=flags)
        return new_cls

    def remove(self, minio):
        """
        Remove remote resource from minio storage

        :param minio: minio instance
        :type minio: :py:class:`minio.Minio`
        """
        minio.remove_object(self.bucket, self.uid)

    def download(self, minio):
        """
        Download RemoteResource into object for local usage

        You probably don't want to use it on your own, rather use :py:meth:`karton.Karton.download_resource` method

        :param minio: minio instance
        :type minio: :py:class:`minio.Minio`
        :rtype: :py:class:`karton.Resource`
        :return: local resource
        """
        reader = minio.get_object(self.bucket, self.uid)
        sio = BytesIO(reader.data)
        content = sio.getvalue()

        size = len(content)

        return Resource(self.name, sio.getvalue(), size, self.uid)

    def download_content_to_file(self, minio, file_path):
        """
        Download RemoteResource into local filesystem with given file_path.

        :param minio: minio instance
        :type minio: :py:class:`minio.Minio`
        :param file_path: file path where to store the downloaded file
        :type: str
        :rtype: str
        :return: file path to the created file
        """
        minio.fget_object(self.bucket, self.uid, file_path)
        return file_path

    def get_size(self, minio):
        """
        Gets size of remote object (without downloading content)

        :param minio: minio instance
        :type minio: :py:class:`minio.Minio`
        :rtype: int
        :return: size of remote object
        """
        stat = minio.stat_object(self.bucket, self.uid)
        return stat.size

    def __repr__(self):
        return str(self.to_dict())


class Resource(RemoteResource):
    """
    Resource represents local resource.
    self.content stores content of the local resource.

    :param name: name of the resource
    :type name: str
    :param bucket: minio bucket
    :type bucket: str, optional
    :param _uid: uuid
    :type _uid: str, optional
    :param sha256: sha256 of object if known
    :type sha256: str, optional
    """

    def __init__(self, name, content, _uid=None, *args, **kwargs):
        super(Resource, self).__init__(name, _uid=_uid)
        self.content = content

        # Python2 represents binary as str, no need to convert
        if type(content) is str and sys.version_info >= (3, 0):
            content = content.encode("utf-8")
        elif type(content) is bytes:
            pass
        else:
            raise TypeError("Content can be bytes or str only")

        self.sha256 = hashlib.sha256(content).hexdigest()

    @property
    def size(self):
        return len(self.content)

    def remove(self, minio):
        raise LocalResourceCanNotBeRemoved()

    def download(self, minio):
        raise LocalResourceCanNotBeDownloaded()

    def get_size(self, minio):
        return self.size

    def _upload(self, minio, bucket):
        """
        This is where we sync with remote, never to be used by user explicitly
        Should be invoked while uploading task
        """
        if self.content is None:
            raise NoContentException("Resource does not have any content in it")

        if not minio.bucket_exists(bucket):
            minio.make_bucket(bucket_name=bucket)

        content = self.content

        # Python2 represents binary as str, no need to convert
        if type(content) is str and sys.version_info >= (3, 0):
            content = content.encode("utf-8")
        elif type(content) is bytes:
            pass
        else:
            raise TypeError("Content can be bytes or str only")

        content = BytesIO(content)

        minio.put_object(bucket, self.uid, content, len(self.content))

    def upload(self, minio, bucket):
        """
        :param minio: minio instance
        :type minio: :py:class:`minio.Minio`
        :param bucket: bucket to download from
        :type bucket: str
        :rtype: :py:class:`karton.RemoteResource`
        """
        self._upload(minio=minio, bucket=bucket)
        return RemoteResource(self.name, bucket, _uid=self.uid, flags=self.flags)


class RemoteDirectoryResource(RemoteResource):
    """
    Extension of Resource object, allowing for easy interaction with directories
    self._content stores zipfile raw bytes.

    :param name: name of the resource
    :type name: str
    :param bucket: minio bucket
    :type bucket: str, optional
    :param _uid: uuid
    :type _uid: str, optional
    :param sha256: sha256 of object if known
    :type sha256: str, optional
    """

    @contextlib.contextmanager
    def download_to_temporary_folder(self, minio):
        """
        Context manager for using content of the DirResource, this is the preferred way of getting the contents.

        Ensures that the unpacked content is removed after usage.

        You probably don't want to use it on your own, rather use Karton.download_to_temporary_folder method

        :param minio: minio instance
        :type minio: :py:class:`minio.Minio`
        :rtype: str
        :return: path to unpacked contents
        """
        with tempfile.NamedTemporaryFile() as f:
            tmp_file = self.download_content_to_file(minio=minio, file_path=f.name)

            zip_file = zipfile.ZipFile(tmp_file)

            tmpdir = tempfile.mkdtemp()

            try:
                zip_file.extractall(tmpdir)
                yield tmpdir
            finally:
                shutil.rmtree(tmpdir)

    def download_zip_file(self, minio):
        """
        When contextmanager cannot be used, user should handle zipfile himself any way he likes.

        You probably don't want to use it on your own, rather use Karton.download_zip_file method

        :rtype: :py:class:`zipfile.Zipfile`
        :return: zipfile object from content
        """
        resource = self.download(minio=minio)
        content = BytesIO(resource.content)
        return zipfile.ZipFile(content)


class DirectoryResource(RemoteDirectoryResource, Resource):
    """
    Resource specialized in handling directories

    :param name: name of the resource
    :type name: str
    :param directory_path: directory to be compressed and used as a minio object later on
    :type directory_path: str
    :param bucket: minio bucket
    :type bucket: str, optional
    :param _uid: uuid
    :type _uid: str, optional
    :param sha256: sha256 of object if known
    :type sha256: str, optional
    """

    def __init__(self, name, directory_path, *args, **kwargs):
        self.directory_path = directory_path
        content = zip_dir(directory_path).getvalue()

        super(DirectoryResource, self).__init__(name, content, *args, **kwargs)

        self.sha256 = hashlib.sha256(content).hexdigest()
        self.flags = [ResourceFlagEnum.DIRECTORY]

    def upload(self, minio, bucket):
        """
        :param minio: minio instance
        :type minio: :py:class:`minio.Minio`
        :rtype:
        :return: RemoteDirectoryResource to use locally
        """
        self._upload(minio=minio, bucket=bucket)
        return RemoteDirectoryResource(
            self.name, bucket, _uid=self.uid, flags=self.flags
        )


class PayloadBag(dict):
    def directory_resources(self):
        """
        generator for DirectoryResources

        :rtype: Iterator[:py:class:`karton.DirectoryResource`]
        :return: yields single DirectoryResource
        """
        for k, v in self.items():
            if isinstance(v, RemoteResource) and v.is_directory():
                yield (k, v)

    def file_resources(self):
        """
        generator for normal resources that is without DirectoryResources

        :rtype: Iterator[:py:class:`karton.Resource`]
        :return: yields single resource (excluding DirectoryResources)
        """
        for k, v in self.items():
            if isinstance(v, RemoteResource) and not v.is_directory():
                yield (k, v)

    def resources(self):
        """
        generator for normal resources - that is without DirectoryResources

        :rtype: Iterator[:py:class:`karton.DirectoryResource`]
        :return: yields single resource
        """
        for k, v in self.items():
            if isinstance(v, RemoteResource):
                yield (k, v)
