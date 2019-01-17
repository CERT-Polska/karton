"""
Base library for karton subsystems.
"""
import contextlib
import json
import shutil
import tempfile
import traceback
import uuid
import sys
import os
import zipfile
from enum import Enum

from config import minio_config

import pika
import logging
from minio import Minio
from tempfile import NamedTemporaryFile

from io import StringIO, BytesIO


class RabbitMQHandler(logging.Handler):
    def __init__(self, parameters):
        logging.Handler.__init__(self)
        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()
        self.task_id = 'unknown'

    def set_task_id(self, task_id):
        self.task_id = task_id

    def emit(self, record: logging.LogRecord):
        ignore_fields = ["args", "asctime", "msecs", "msg", "pathname", "process", "processName", "relativeCreated",
                         "exc_info", "exc_text", "stack_info", "thread", "threadName"]
        log_line = {k: v for k, v in record.__dict__.items() if k not in ignore_fields}
        if record.exc_info:
            log_line["excText"] = logging.Formatter().formatException(record.exc_info)
            log_line["excValue"] = str(record.exc_info[1])
            log_line["excType"] = record.exc_info[0].__name__
            log_line["excTraceback"] = traceback.format_exception(*record.exc_info)

        log_line["type"] = "log"
        log_line["taskId"] = self.task_id

        try:
            self.channel.basic_publish("karton.logs", "", json.dumps(log_line), pika.BasicProperties())
        except pika.exceptions.ChannelClosed:
            pass

    def close(self):
        self.connection.close()


class KartonBaseService:
    identity = None

    def __init__(self, parameters):
        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()
        self.current_task = None

        self.log = logging.getLogger(self.identity)
        self.log.setLevel(logging.DEBUG)
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(logging.Formatter("[%(asctime)s][%(levelname)s] %(message)s"))
        self.rmq_handler = RabbitMQHandler(parameters)
        self.log.addHandler(stream_handler)
        self.log.addHandler(self.rmq_handler)

    def process(self):
        raise RuntimeError("Not implemented.")

    def send_create_task(self, task):
        op_json = json.dumps({
            "type": "create_task",
            "event": {
                "identity": self.identity,
                "uid_stack": task.uid_stack,
                "uid": task.uid,
                "resources": [resource.to_dict() for resource in task.resources]
            }
        })

        self.channel.basic_publish("karton.operations", "", op_json, pika.BasicProperties())

    def send_task(self, task):
        self.send_create_task(task)
        task_json = task.serialize()
        self.channel.basic_publish("karton.tasks", "", task_json, pika.BasicProperties(headers=task.headers))

    def internal_process(self, channel, method, properties, body):
        msg = json.loads(body)
        self.current_task = Task.unserialize(properties.headers, msg)
        self.rmq_handler.set_task_id(self.current_task.uid)

        try:
            self.process()
        except Exception as e:
            self.log.exception("Failed to process task")

    def loop(self):
        self.channel.basic_consume(self.internal_process, queue=self.identity, no_ack=True)
        self.channel.start_consuming()

    def close(self):
        self.connection.close()


class Task:
    def __init__(self, headers, resources, payload):
        """
        Create new root Task.
        """
        if resources is None:
            resources = []
        if payload is None:
            payload = []
        self.uid_stack = [str(uuid.uuid4())]

        self.headers = headers
        self.resources = resources
        self.payload = payload

    @property
    def uid(self):
        return self.uid_stack[-1]

    def derive_task(self, headers=None, resources=None, payload=None):
        """
        Derive new Task which is a child of this Task.
        """
        task = Task(headers or self.headers, resources or self.resources, payload or self.payload)
        task.uid_stack = self.uid_stack + task.uid_stack
        return task

    def serialize(self):
        return json.dumps({"uid_stack": self.uid_stack,
                           "resources": [res.to_dict() for res in self.resources],
                           "payload": self.payload})

    @staticmethod
    def unserialize(headers, data):
        task = Task(headers, data["resources"], data["payload"])
        task.uid_stack = data["uid_stack"]

        resources = [Resource._from_dict(x) for x in data["resources"]]
        task = Task(headers, data["payload"], resources)
        task.uid = data["uid"]
        return task

    def __repr__(self):
        return self.serialize()

    def add_resource(self, resource):
        self.resources.append(resource)

    def _upload_resources(self):
        for resource in self.resources:
            resource._upload()


class NoContentException(Exception):
    pass

class ContentDoesntExist(Exception):
    pass

class ResourceFlagEnum(Enum):
    DIRECTORY = "Directory"


class Resource:
    def __init__(self, name, content=None, bucket=minio_config["bucket"], _uid=None):
        if _uid is None:
            _uid = str(uuid.uuid4())

        self.name = name
        self.uid = _uid
        self._content = content
        self.bucket = bucket

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
            minio = MinioSingleton.instance()

            reader = minio.get_object(self.bucket, self.uid)
            sio = StringIO(reader.data)
            self._content = sio.getvalue()
            self.log.debug("Downloaded content")
        return self._content

    def is_directory(self):
        return ResourceFlagEnum.DIRECTORY in self.flags

    def to_dict(self):
        return {"uid": self.uid, "name": self.name, "bucket": self.bucket}

    def serialize(self):
        return json.dumps(self.to_dict())

    @classmethod
    def unserialize(cls, json_data):
        data = json.load(json_data)
        bucket = data["bucket"]
        name = data["name"]
        _uid = data["uid"]
        return cls(name, None, bucket, _uid=_uid)

    @classmethod
    def from_directory(cls, name, directory_path, bucket=minio_config["bucket"]):
        """
        Alternative constructor for creating resource from directory
        :param name: name of the resource
        :param directory_path: directory to be compressed and used as a minio object later on
        :param bucket: minio bucket
        :return: new instance of Resource
        """
        _content = zip_dir(directory_path)
        return cls(name, _content.getvalue(), bucket, flags=[ResourceFlagEnum.DIRECTORY])

    @classmethod
    def _from_dict(cls, data_dict):
        bucket = data_dict["bucket"]
        name = data_dict["name"]
        _uid = data_dict["uid"]
        return cls(name, None, bucket, _uid=_uid)

    def _upload(self):
        if self._content is None:
            raise NoContentException("Resource does not have any content in it")
        minio = MinioSingleton.instance()
        minio.put_object(self.bucket, self.uid, self._content, len(self._content))
        self.log.debug("Uploaded")

    def __repr__(self):
        return self.serialize()

class DirResource(Resource):
    def __init__(self, name, directory_path=None, bucket=minio_config["bucket"], _uid=None):
        self.flags = [ResourceFlagEnum.DIRECTORY]
        self._path = None
        content = None

        if directory_path is not None:
            content = zip_dir(directory_path)
        super(name, content, bucket, _uid)

    @property
    def content(self):
        raise NotImplementedError("DirResource doesn't support content field, use path contextmanager instead to access directory")

    @contextlib.contextmanager
    def path(self):
        if self._content is None:
            raise ContentDoesntExist("No content was found in the DirResource")

        tmpdir = tempfile.mkdtemp()
        z = zipfile.ZipFile(self._content)
        z.extractall(tmpdir)
        try:
            yield tmpdir
        finally:
            shutil.rmtree(tmpdir)





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


class MinioSingleton:
    mini = None
    @classmethod
    def instance(cls):
        if cls.minio is None:
            cls.minio = Minio(minio_config["address"], minio_config["access_key"], minio_config["secret_key"], secure=True)
        return cls.minio