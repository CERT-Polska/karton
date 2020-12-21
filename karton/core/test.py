"""
Test stubs for karton subsystem unit tests
"""
import configparser
import hashlib
import logging
import unittest
from typing import Any, BinaryIO, Dict, List, Optional, Union, cast
from collections import defaultdict

from .backend import KartonMetrics
from .karton import Config
from .resource import Resource, ResourceBase
from .task import Task, TaskState
from .utils import get_function_arg_num
from unittest import mock


__all__ = ["KartonTestCase", "mock"]


log = logging.getLogger()


class ConfigMock(Config):
    def __init__(self) -> None:
        self.config = configparser.ConfigParser()


class KartonBackendMock:
    def __init__(self) -> None:
        self.produced_tasks = []
        # A custom MinIO system mock
        self.buckets: Dict[str, bytes] = defaultdict(dict)

    @property
    def default_bucket_name(self) -> str:
        return "karton.test"

    def register_task(self, task: Task) -> None:
        log.debug("Registering a new task in Redis: %s", task.serialize())

    def set_task_status(
        self, task: Task, status: TaskState, consumer: Optional[str] = None
    ) -> None:
        log.debug("Setting task %s status to %s", task.uid, status)

    def produce_unrouted_task(self, task: Task) -> None:
        log.debug("Producing a new unrouted task")
        self.produced_tasks.append(task)

    def produce_log(
        self, log_record: Dict[str, Any], logger_name: str, level: str,
    ) -> bool:
        log.debug("Producing a log from [%s]: %s", logger_name, log_record)
        # Return a truthy value to signal that the message has been consumed
        return True

    def increment_metrics(self, metric: KartonMetrics, identity: str) -> None:
        log.debug("Incrementing metric %s for identity %s", metric, identity)

    def remove_object(self, bucket: str, object_uid: str) -> None:
        del self.buckets[bucket][object_uid]

    def upload_object(
        self,
        bucket: str,
        object_uid: str,
        content: Union[bytes, BinaryIO],
        length: int = None,
    ) -> None:
        self.buckets[bucket][object_uid] = content

    def download_object(self, bucket: str, object_uid: str) -> bytes:
        return self.buckets[bucket][object_uid]

    def upload_object_from_file(self, bucket: str, object_uid: str, path: str) -> None:
        with open(path, "rb") as f:
            self.buckets[bucket][object_uid] = f.read()

    def download_object_to_file(self, bucket: str, object_uid: str, path: str) -> None:
        with open(path, "wb") as f:
            f.write(self.buckets[bucket][object_uid])


class KartonTestCase(unittest.TestCase):
    """
    Unit test case class

    .. code-block:: python
        from cutter import Cutter

        class CutterTestCase(KartonTestCase):
            karton_class = Cutter

        def test_karton_service(self):
            resource = Resource('incoming', b'put content here')
            task = Task({
                'type': 'string'
            }, payload={
                'chars': 6,
                'sample': resource
            })
            results = self.run_task(task)
            self.assertTasksEqual(results, [
                Task({
                    'origin': 'karton.cutter',
                    'type': 'cutted_string'
                }, payload={
                    'sample': Resource('outgoing', b'put co')
                })
            ])
    """

    karton_class = None
    config = None
    kwargs = None

    def setUp(self) -> None:
        kwargs: Dict[Any, Any] = self.kwargs or {}
        self.karton = self.karton_class(  # type: ignore
            config=ConfigMock(), backend=KartonBackendMock(), **kwargs
        )

    def get_resource_sha256(self, resource: ResourceBase) -> str:
        """Calculate SHA256 hash for a given resource

        :param resource: Resource to be hashed
        :return: Hexencoded SHA256 digest
        """
        h = hashlib.sha256()
        if resource._path is not None:
            with open(resource._path, "rb") as f:
                while True:
                    block = f.read(65536)
                    if not block:
                        break
                    h.update(block)
        else:
            h.update(cast(bytes, resource._content))
        return h.hexdigest()

    def assertResourceEqual(
        self, resource: ResourceBase, expected: ResourceBase, resource_name: str
    ) -> None:
        """Assert that two resources are equal

        :param resource: Output resource
        :param expected: Expected resource
        :param resource_name: Resource name
        """
        self.assertTrue(
            isinstance(resource, ResourceBase),
            "Resource type mismatch in {}".format(resource_name),
        )
        self.assertEqual(
            self.get_resource_sha256(resource),
            self.get_resource_sha256(expected),
            "Resource content mismatch in {}".format(resource_name),
        )

    def assertPayloadBagEqual(
        self, payload: Dict[str, Any], expected: Dict[str, Any], payload_bag_name: str
    ) -> None:
        """Assert that two payload bags are equal

        :param payload: Result payload bag
        :param expected: Expected payload bag
        :param payload_bag_name: Bag name
        """
        self.assertSetEqual(
            set(payload.keys()),
            set(expected.keys()),
            "Incorrect fields set in {}".format(payload_bag_name),
        )
        for key, value in payload.items():
            other_value = expected[key]
            path = "{}.{}".format(payload_bag_name, key)
            if not isinstance(value, ResourceBase):
                self.assertEqual(
                    value, other_value, "Incorrect value of {}".format(path),
                )
            else:
                self.assertResourceEqual(value, other_value, path)

    def assertTaskEqual(self, task: Task, expected: Task) -> None:
        """Assert that two tasks objects are equal
        :param task: Result task
        :param expected: Expected task
        """
        self.assertDictEqual(task.headers, expected.headers, "Headers mismatch")
        self.assertPayloadBagEqual(task.payload, expected.payload, "payload")
        self.assertPayloadBagEqual(
            task.payload_persistent, expected.payload_persistent, "payload_persistent"
        )

    def assertTasksEqual(self, tasks: List[Task], expected: List[Task]) -> None:
        """
        Assert that two task lists are equal
        :param tasks: Result tasks list
        :param expected: Expected tasks list
        """
        self.assertEqual(len(tasks), len(expected), "Incorrect number of tasks sent")
        for task, other in zip(tasks, expected):
            self.assertTaskEqual(task, other)

    def run_task(self, task: Task) -> List[Task]:
        """
        Spawns task into tested Karton subsystem instance
        :param task: Task to be spawned
        :return: Result tasks sent by Karton Service
        """
        self.karton.backend.produced_tasks = []
        self.karton.current_task = task

        if get_function_arg_num(self.karton.process) == 0:
            self.karton.process()
        else:
            self.karton.process(self.karton.current_task)  # type: ignore

        return self.karton.backend.produced_tasks


# Backward compatibility
TestResource = Resource
