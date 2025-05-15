"""
Test stubs for karton subsystem unit tests
"""

import hashlib
import logging
import unittest
from collections import defaultdict
from typing import Any, BinaryIO, Dict, List, Optional, Union, cast
from unittest import mock

from .backend import KartonBackend, KartonMetrics
from .config import Config
from .resource import LocalResource, RemoteResource, ResourceBase
from .task import Task, TaskState

__all__ = ["KartonTestCase", "mock"]


log = logging.getLogger()


class ConfigMock(Config):
    def __init__(self):
        self._config = {"redis": {}, "s3": {}}


class BackendMock:
    def __init__(self) -> None:
        self.produced_tasks: List[Task] = []
        # A custom S3 system mock
        self.buckets: Dict[str, Dict[str, bytes]] = defaultdict(dict)

    @property
    def default_bucket_name(self) -> str:
        return "karton.test"

    def register_task(self, task: Task, pipe=None) -> None:
        log.debug("Registering a new task in Redis: %s", task.serialize())

    def set_task_status(self, task: Task, status: TaskState, pipe=None) -> None:
        log.debug("Setting task %s status to %s", task.uid, status)

    def produce_unrouted_task(self, task: Task) -> None:
        log.debug("Producing a new unrouted task")
        self.produced_tasks.append(task)

    def produce_log(
        self,
        log_record: Dict[str, Any],
        logger_name: str,
        level: str,
    ) -> bool:
        log.debug("Producing a log from [%s]: %s", logger_name, log_record)
        # Return a truthy value to signal that the message has been consumed
        return True

    def increment_metrics(self, metric: KartonMetrics, identity: str) -> None:
        log.debug("Incrementing metric %s for identity %s", metric, identity)

    def remove_object(self, bucket: str, object_uid: str) -> None:
        log.debug("Deleting object %s from bucket %s", object_uid, bucket)
        del self.buckets[bucket][object_uid]

    def upload_object(
        self,
        bucket: str,
        object_uid: str,
        content: Union[bytes, BinaryIO],
        length: Optional[int] = None,
    ) -> None:
        log.debug("Uploading object %s to bucket %s", object_uid, bucket)
        if isinstance(content, bytes):
            self.buckets[bucket][object_uid] = content
        else:
            self.buckets[bucket][object_uid] = content.read()

    def download_object(self, bucket: str, object_uid: str) -> bytes:
        log.debug("Downloading object %s from bucket %s", object_uid, bucket)
        return self.buckets[bucket][object_uid]

    def upload_object_from_file(self, bucket: str, object_uid: str, path: str) -> None:
        log.debug("Uploading object %s from file from bucket %s", object_uid, bucket)
        with open(path, "rb") as f:
            self.buckets[bucket][object_uid] = f.read()

    def download_object_to_file(self, bucket: str, object_uid: str, path: str) -> None:
        log.debug("Downloading object %s from bucket %s to file", object_uid, bucket)
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
        if self.config is None:
            self.config = ConfigMock()
        self.backend = BackendMock()
        self.karton = self.karton_class(  # type: ignore
            config=self.config, backend=self.backend, **kwargs
        )

    def get_resource_sha256(self, resource: ResourceBase) -> str:
        """
        Calculate SHA256 hash for a given resource

        :param resource: Resource to be hashed
        :return: Hex-encoded SHA256 digest
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
            h.update(cast(bytes, resource.content))
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

    def assertTaskEqual(self, task: Task, expected: Task) -> None:
        """
        Assert that two task objects are equal

        :param task: Result task
        :param expected: Expected task
        """
        self.assertDictEqual(task.headers, expected.headers, "Headers mismatch")
        # Get paths and values sorted by path
        task_payload_values = sorted(task.walk_payload_items(), key=lambda el: el[0])
        expected_payload_values = sorted(
            expected.walk_payload_items(), key=lambda el: el[0]
        )
        self.assertSetEqual(
            set(path for path, _ in task_payload_values),
            set(path for path, _ in expected_payload_values),
        )
        for task_payload, expected_payload in zip(
            task_payload_values, expected_payload_values
        ):
            task_key, task_value = task_payload
            expected_key, expected_value = expected_payload
            assert (
                task_key == expected_key
            )  # If not true, there is something wrong with this test routine
            if not isinstance(task_value, ResourceBase):
                self.assertEqual(
                    task_value, expected_value, "Incorrect value of {}".format(task_key)
                )
            else:
                self.assertResourceEqual(task_value, expected_value, task_key)

    def assertTasksEqual(self, tasks: List[Task], expected: List[Task]) -> None:
        """
        Assert that two task lists are equal

        :param tasks: Result tasks list
        :param expected: Expected tasks list
        """
        self.assertEqual(len(tasks), len(expected), "Incorrect number of tasks sent")
        for task, other in zip(tasks, expected):
            self.assertTaskEqual(task, other)

    def _process_task(self, incoming_task: Task):
        """
        Converts task from outgoing to incoming including transformation
        of LocalResources to RemoteResources
        """
        task = incoming_task.fork_task()
        task.status = TaskState.STARTED
        task.headers.update({"receiver": self.karton.identity})

        def local_resource_to_remote(obj):
            if not isinstance(obj, ResourceBase):
                return obj
            if not isinstance(obj, LocalResource):
                raise ValueError("Test task must contain only LocalResource objects")
            backend = cast(KartonBackend, self.backend)
            resource = cast(LocalResource, obj)
            resource.bucket = backend.default_bucket_name
            resource.upload(backend)
            return RemoteResource(
                name=resource.name,
                bucket=resource.bucket,
                metadata=resource.metadata,
                uid=resource.uid,
                size=resource.size,
                backend=backend,
                sha256=resource.sha256,
                _flags=resource._flags,
            )

        task.transform_payload_bags(local_resource_to_remote)
        return task

    def run_task(self, task: Task) -> List[Task]:
        """
        Spawns task into tested Karton subsystem instance

        :param task: Task to be spawned
        :return: Result tasks sent by Karton Service
        """
        self.karton.backend.produced_tasks = []
        outgoing_task = self._process_task(task)
        self.karton.current_task = outgoing_task

        self.karton.process(self.karton.current_task)

        return self.karton.backend.produced_tasks


# Backward compatibility
TestResource = LocalResource
