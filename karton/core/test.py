"""
Test stubs for karton subsystem unit tests
"""
import contextlib
import hashlib
import logging
import shutil
import tempfile
import time
import unittest
import zipfile
from io import BytesIO
from typing import IO, Any, Dict, Iterator, List, Optional, cast
from unittest import mock

from .karton import Consumer
from .resource import ResourceBase
from .task import Task
from .utils import get_function_arg_num

__all__ = ["KartonTestCase", "mock"]


class TestResource(ResourceBase):
    """
    LocalResource imitating RemoteResource for test purposes.

    Should be used in test cases instead of LocalResource objects.

    Passing resource as path is not supported.

    :param name: Name of the resource (e.g. name of file)
    :type name: str
    :param content: Resource content
    :type content: bytes or str
    :param metadata: Resource metadata
    :type metadata: dict, optional
    """

    def download(self) -> Optional[bytes]:
        return self._content

    def unload(self) -> None:
        # Just ignore that call
        pass

    def download_to_file(self, path: str) -> None:
        if not self._content:
            raise RuntimeError(
                "Cannot download data to a file because content is set to none"
            )

        with open(path, "wb") as f:
            f.write(self._content)

    @contextlib.contextmanager
    def download_temporary_file(self) -> Iterator[IO[bytes]]:
        with tempfile.NamedTemporaryFile() as f:
            self.download_to_file(f.name)
            yield f

    @contextlib.contextmanager
    def zip_file(self) -> Iterator[zipfile.ZipFile]:
        if self._content is None:
            raise RuntimeError(
                "Cannot zipfile resource contents because they are set to none"
            )

        yield zipfile.ZipFile(BytesIO(self._content))

    def extract_to_directory(self, path: str) -> None:
        with self.zip_file() as zf:
            zf.extractall(path)

    @contextlib.contextmanager
    def extract_temporary(self) -> Iterator[str]:
        tmpdir = tempfile.mkdtemp()
        try:
            self.extract_to_directory(tmpdir)
            yield tmpdir
        finally:
            shutil.rmtree(tmpdir)


class KartonTestCase(unittest.TestCase):
    """
    Unit test case class

    .. code-block:: python
        from cutter import Cutter

        class CutterTestCase(KartonTestCase):
            karton_class = Cutter

        def test_karton_service(self):
            resource = TestResource('incoming', b'put content here')
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

    longMessage = True

    @classmethod
    def setUpClass(cls) -> None:
        cls._karton_mock = KartonMock.from_karton(cls.karton_class)  # type: ignore

    def setUp(self) -> None:
        kwargs: Dict[Any, Any] = self.kwargs or {}
        self.karton = self._karton_mock(self.config, **kwargs)  # type: ignore

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
                    value,
                    other_value,
                    "Incorrect value of {}".format(path),
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
        for key, resource in task.iterate_resources():
            if not isinstance(resource, TestResource):
                raise TypeError(
                    "Input resources must be instantiated using TestResource "
                    "instead of plain LocalResource (payload key: '{}')".format(key)
                )
        return self.karton.run_task(task)


class KartonMock(object):
    identity = ""
    filters: List[dict] = []

    @classmethod
    def from_karton(cls, karton_class):
        """
        Turns Consumer into its mocked version, so we can test subsystem
        without interaction with infrastructure

        :param karton_class: Consumer-based Karton class
        :type karton_class: Type[Consumer]
        :return: Type[KartonMock]
        """
        if not issubclass(karton_class, Consumer):
            raise TypeError("Karton must be karton.Consumer subclass")

        if "internal_process" in karton_class.__dict__:
            raise TypeError("Karton can't override internal_process method")

        return type(karton_class.__name__ + "Mock", (cls,), dict(karton_class.__dict__))

    def __init__(self, config, **kwargs) -> None:
        self.config = config
        self.current_task: Optional[Task] = None
        self._result_tasks: List[Task] = []

    @property
    def log(self) -> logging.Logger:
        return logging.getLogger(self.identity)

    def send_task(self, task: Task) -> None:
        """Mock the normal send_task and instead just append it to an internal
        list of output tasks

        :param task: The Task object to send
        """
        self.log.debug("Dispatched task %s", task.uid)

        # Complete information about task
        if self.current_task is not None:
            task.set_task_parent(self.current_task)
            task.merge_persistent_payload(self.current_task)
            task.priority = self.current_task.priority

        task.last_update = time.time()
        task.headers.update({"origin": self.identity})

        self._result_tasks.append(task)

    def process(self) -> None:
        """
        Expected to be overwritten

        self.current_task contains task that triggered invocation of
        :py:meth:`karton.Consumer.process`
        """
        raise NotImplementedError()

    def run_task(self, task: Task) -> List[Task]:
        """
        Provides task for processing and compares result tasks with expected ones

        :param task: Input task for subsystem
        :return: List of generated tasks
        """
        self._result_tasks = []
        self.current_task = task
        if not self.current_task.matches_filters(self.filters):
            raise RuntimeError("Provided task doesn't match any of filters")
        if get_function_arg_num(self.process) == 0:
            self.process()
        else:
            self.process(self.current_task)  # type: ignore
        return self._result_tasks
