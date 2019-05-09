import contextlib
import json
import shutil
import tempfile
import uuid
import zipfile
import sys
from io import BytesIO, StringIO

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
    def __init__(self, name, bucket=None, _uid=None):
        if _uid is None:
            _uid = str(uuid.uuid4())

        self.name = name
        self.bucket = bucket
        self.uid = _uid

        self.flags = []

    def is_directory(self):
        """
        Helps to identify DirectoryResource vs Resource without type checking

        :return: true if this instance is RemoteDirectoryResource or DirectoryResource
        """
        # both conditions should be identical
        return ResourceFlagEnum.DIRECTORY in self.flags or isinstance(self, RemoteDirectoryResource)

    def to_dict(self):
        return {"uid": self.uid, "name": self.name, "bucket": self.bucket, "flags": self.flags}

    def serialize(self):
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, data_dict):
        bucket = data_dict["bucket"]
        name = data_dict["name"]
        _uid = data_dict["uid"]
        flags = data_dict["flags"]

        new_cls = cls(name, bucket, _uid=_uid)
        new_cls.flags = flags
        return new_cls

    def remove(self, minio):
        """
        Remove remote resource from minio storage

        :param minio: minio instance
        """
        minio.remove_object(self.bucket, self.uid)

    def download(self, minio):
        """
        Download RemoteResource into object for local usage

        :param minio: minio instance
        :return: :py:class:`karton.Resource` - local resource
        """
        reader = minio.get_object(self.bucket, self.uid)
        sio = BytesIO(reader.data)
        content = sio.getvalue()

        size = len(content)

        return Resource(self.name, sio.getvalue(), size, self.uid)

    def get_size(self, minio):
        """
        Gets size of remote object (without downloading content)

        :param minio: minio instance
        :return: size of remote object
        """
        stat = minio.stat_object(self.bucket, self.name)
        return stat.size

    def __repr__(self):
        return str(self.to_dict())


class Resource(RemoteResource):
    def __init__(self, name, content, size=None, _uid=None):
        super(Resource, self).__init__(name, _uid=_uid)
        self.content = content
        self.size = len(content) if content is not None else size

    def remove(self, minio):
        raise LocalResourceCanNotBeRemoved()

    def download(self, minio):
        raise LocalResourceCanNotBeDownloaded()

    def get_size(self, minio):
        return len(self.content)

    def _upload(self, minio, bucket):
        """
        This is where we sync with remote, never to be used by user explicitly
        Should be invoked while uploading task
        """
        if self.content is None:
            raise NoContentException("Resource does not have any content in it")

        if bucket and not minio.bucket_exists(bucket):
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
        :return: RemoteResource to use locally
        """
        self._upload(minio=minio, bucket=bucket)

        rr = RemoteResource(self.name, bucket, _uid=self.uid)
        rr.flags = self.flags
        return rr


class RemoteDirectoryResource(RemoteResource):
    """
    Extension of Resource object, allowing for easy interaction with directories
    self._content stores zipfile raw bytes.

    Content extraction should be done through path or zip_file.
    """

    @contextlib.contextmanager
    def download_to_temporary_folder(self, minio):
        """
        Context manager for using content of the DirResource, this is the preferred way of getting the contents.

        Ensures that the unpacked content is removed after usage.

        :param minio: minio instance
        :return: path to unpacked contents
        """
        resource = self.download(minio=minio)
        content = BytesIO(resource.content)

        zip_file = zipfile.ZipFile(content)

        tmpdir = tempfile.mkdtemp()
        zip_file.extractall(tmpdir)
        try:
            yield tmpdir
        finally:
            shutil.rmtree(tmpdir)

    def download_zip_file(self, minio):
        """
        When contextmanager cannot be used, user should handle zipfile himself any way he likes.

        :return: zipfile object from content
        """
        resource = self.download(minio=minio)
        content = BytesIO(resource.content)
        return zipfile.ZipFile(content)


class DirectoryResource(RemoteDirectoryResource, Resource):
    def __init__(self, name, directory_path, *args, **kwargs):
        """
        Resource specialized in handling directories

        :param name: name of the resource
        :param directory_path: directory to be compressed and used as a minio object later on
        :param bucket: minio bucket
        :param _uid: uuid
        :return: new instance of DirResource
        """
        content = zip_dir(directory_path).getvalue()

        super(DirectoryResource, self).__init__(name, content, *args, **kwargs)

        self.flags = [ResourceFlagEnum.DIRECTORY]

    def upload(self, minio, bucket):
        """
        :return: RemoteDirectoryResource to use locally
        """
        self._upload(minio=minio, bucket=bucket)

        rr = RemoteDirectoryResource(self.name, bucket, _uid=self.uid)
        rr.flags = self.flags
        return rr


class PayloadBag(dict):
    def directory_resources(self):
        """
        generator for DirectoryResources

        :return: yields simple DirectoryResource
        """
        for k, v in self.items():
            if isinstance(v, RemoteResource) and v.is_directory():
                yield (k, v)

    def file_resources(self):
        """
        generator for normal resources that is without DirectoryResources

        :return: yields single resource
        """
        for k, v in self.items():
            if isinstance(v, RemoteResource) and not v.is_directory():
                yield (k, v)

    def resources(self):
        """
        generator for normal resources - that is without DirectoryResources

        :return: yields single resource
        """
        for k, v in self.items():
            if isinstance(v, RemoteResource):
                yield (k, v)
