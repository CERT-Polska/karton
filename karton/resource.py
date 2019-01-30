import contextlib
import json
import logging
import os
import shutil
import tempfile
import uuid
import zipfile
from enum import Enum
from io import BytesIO

from minio import Minio


class NoContentException(Exception):
    pass


class ContentDoesntExist(Exception):
    pass


class NotConfiguredResource(Exception):
    pass


class ResourceFlagEnum(str, Enum):
    DIRECTORY = "Directory"


class Resource(object):
    """
    Abstraction over remote minio objects.

    This exists to make it easier to share resources across clients
    """
    def __init__(self, name, content=None, bucket=None, _uid=None, config=None):
        if _uid is None:
            _uid = str(uuid.uuid4())

        if config is None:
            raise NotConfiguredResource("Provide config for resource")
        self.config = config

        self.minio = Minio(self.config["address"],
                           self.config["access_key"],
                           self.config["secret_key"],
                           secure=bool(int(self.config.get("secure", True))))
        if bucket and not self.minio.bucket_exists(bucket):
            self.minio.make_bucket(bucket_name=bucket)

        self.name = name
        self.uid = _uid
        self._content = content
        self.bucket = bucket
        self.flags = []

        self.log = logging.getLogger(self.name)
        self.log.setLevel(logging.DEBUG)

    @property
    def content(self):
        """
        Resources are just abstractions on minio objects, we want to download them lazily due to the fact that many
        services are not gonna use them anyway.
        :return: content of the resource
        """
        if self._content is None:
            reader = self.minio.get_object(self.bucket, self.uid)
            sio = BytesIO(reader.data)
            self._content = sio.getvalue()
            self.log.debug("Downloaded content")
        return self._content

    def is_directory(self):
        """
        Helps to identify DirResource vs Resource without type checking
        :return: true if we are DirResource
        """
        # both conditions should be identical
        return ResourceFlagEnum.DIRECTORY in self.flags or isinstance(self, DirResource)

    def to_dict(self):
        return {"uid": self.uid, "name": self.name, "bucket": self.bucket, "flags": self.flags}

    def serialize(self):
        return json.dumps(self.to_dict())

    @classmethod
    def _from_dict(cls, data_dict, config=None):
        bucket = data_dict["bucket"]
        name = data_dict["name"]
        _uid = data_dict["uid"]
        flags = data_dict["flags"]

        new_cls = cls(name, None, bucket, _uid=_uid, config=config)
        new_cls.flags = flags
        return new_cls

    def _upload(self):
        """
        This is where we sync with remote, never to be used by user explicitly
        Should be invoked while uploading task
        :return: None
        """
        if self._content is None:
            raise NoContentException("Resource does not have any content in it")
        print(type(self._content))
        self.minio.put_object(self.bucket, self.uid, BytesIO(self._content), len(self._content))
        self.log.debug("Uploaded")

    def _remove(self):
        self.minio.remove_object(self.bucket, self.uid)
        self.log.debug("Removed")

    def __repr__(self):
        return self.serialize()


class DirResource(Resource):
    """
    Extension of Resource object, allowing for easy interaction with directories
    self._content stores zipfile raw bytes.

    Content extraction should be done through path or zip_file.
    """
    def __init__(self, name, directory_path=None, bucket=None, _uid=None, config=None):
        """
        :param name: name of the resource
        :param directory_path: directory to be compressed and used as a minio object later on
        :param bucket: minio bucket
        :param _uid: uuid
        :return: new instance of DirResource
        """
        content = None

        if directory_path is not None:
            content = zip_dir(directory_path).getvalue()

        super(DirResource, self).__init__(name, content, bucket, _uid, config=config)

        self.flags = [ResourceFlagEnum.DIRECTORY]

    @contextlib.contextmanager
    def path(self):
        """
        Context manager for using content of the DirResource, this is the preferred way of getting the contents.

        Ensures that the unpacked content is removed after usage.
        :return: path to unpacked contents
        """
        z = zipfile.ZipFile(self.content)
        tmpdir = tempfile.mkdtemp()
        z.extractall(tmpdir)
        try:
            yield tmpdir
        finally:
            shutil.rmtree(tmpdir)

    @property
    def zip_file(self):
        """
        When contextmanager cannot be used, user should handle zipfile himself any way he likes.
        :return: zipfile object from content
        """
        return zipfile.ZipFile(self.content)


def zip_dir(directory):
    result = BytesIO()
    dlen = len(directory)
    with zipfile.ZipFile(result, "w") as zf:
        for root, dirs, files in os.walk(directory):
            for name in files:
                full = os.path.join(root, name)
                rel = root[dlen:]
                dest = os.path.join(rel, name)
                zf.write(full, dest)
    return result



