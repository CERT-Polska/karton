import contextlib
import json
import shutil
import tempfile
import uuid
import zipfile
from enum import Enum
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


class ResourceFlagEnum(str, Enum):
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
        Helps to identify DirResource vs Resource without type checking
        :return: true if we are RemoteDirResource
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
        minio.remove_object(self.bucket, self.uid)

    def download(self, minio):
        """
        Download RemoteResource into object for local usage
        :param minio:
        :return:
        """
        reader = minio.get_object(self.bucket, self.uid)
        sio = BytesIO(reader.data)
        content = sio.getvalue()

        size = len(content)

        return Resource(self.name, sio.getvalue(), size, self.uid)

    def get_size(self, minio):
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

    def upload(self, minio, bucket):
        """
        This is where we sync with remote, never to be used by user explicitly
        Should be invoked while uploading task
        :return: RemoteResource to use locally
        """
        if self.content is None:
            raise NoContentException("Resource does not have any content in it")

        if bucket and not minio.bucket_exists(bucket):
            minio.make_bucket(bucket_name=bucket)

        minio.put_object(bucket, self.uid, BytesIO(self.content), len(self.content))

        return RemoteResource(self.name, bucket, _uid=self.uid)


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
        :return: path to unpacked contents
        """
        resource = self.download(minio=minio)
        zip_file = zipfile.ZipFile(resource.content)

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
        r = self.download(minio=minio)
        return zipfile.ZipFile(r.content)


class DirectoryResource(RemoteDirectoryResource, Resource):
    def __init__(self, name, bucket, directory_path, *args, **kwargs):
        """
        :param name: name of the resource
        :param directory_path: directory to be compressed and used as a minio object later on
        :param bucket: minio bucket
        :param _uid: uuid
        :return: new instance of DirResource
        """
        content = zip_dir(directory_path).getvalue()

        super(DirectoryResource, self).__init__(name, bucket, content, *args, **kwargs)

        self.flags = [ResourceFlagEnum.DIRECTORY]


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
        generator for normal resources that is without DirectoryResources
        :return: yields single resource
        """
        for k, v in self.items():
            if isinstance(v, RemoteResource):
                yield (k, v)

