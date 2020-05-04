"""
Test stubs for karton subsystem unit tests
"""
import contextlib
import logging
import hashlib
import shutil
import unittest
import tempfile
import time
import zipfile

from .karton import Consumer
from .resource import (
    RemoteResource,
    RemoteDirectoryResource,
    ResourceBase,
    DirectoryResourceBase,
)

try:
    from unittest import mock
except ImportError:
    # Py2 compatibility: needs "mock" to be installed
    import mock

__all__ = ["KartonTestCase", "mock"]


class TestResource(RemoteResource):
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

    def __init__(self, name, content, metadata=None):
        super(TestResource, self).__init__(name, metadata=metadata)
        self._content = content

    def download(self):
        return self._content

    def unload(self):
        # Just ignore that call
        pass

    def download_to_file(self, path):
        with open(path, "wb") as f:
            f.write(self._content)

    @contextlib.contextmanager
    def download_temporary_file(self):
        with tempfile.NamedTemporaryFile() as f:
            self.download_to_file(f.name)
            yield f


class TestDirectoryResource(TestResource, RemoteDirectoryResource):
    """
    LocalDirectoryResource imitating RemoteDirectoryResource for test purposes.

    Should be used in test cases instead of LocalDirectoryResource objects.

    :param name: Name of the resource (e.g. name of file)
    :type name: str
    :param content: ZIP file content to be treated as RemoteDirectoryResource
    :type content: bytes
    :param metadata: Resource metadata
    :type metadata: dict, optional
    """

    @contextlib.contextmanager
    def zip_file(self):
        yield zipfile.ZipFile(self._content)

    def extract_to_directory(self, path):
        with self.zip_file() as zf:
            zf.extractall(path)

    @contextlib.contextmanager
    def extract_temporary(self):
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

        def test_kartonik(self):
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
    def setUpClass(cls):
        cls._karton_mock = KartonMock.from_karton(cls.karton_class)

    def setUp(self):
        self.karton = self._karton_mock(self.config, **(self.kwargs or {}))

    def get_resource_sha256(self, resource):
        h = hashlib.sha256()
        if resource._path is not None:
            with open(resource._path, "rb") as f:
                while True:
                    block = f.read(65536)
                    if not block:
                        break
                    h.update(block)
        else:
            h.update(resource._content)
        return h.hexdigest()

    def assertResourceEqual(self, resource, expected, resource_name):
        self.assertEqual(
            isinstance(resource, DirectoryResourceBase),
            isinstance(expected, DirectoryResourceBase),
            "Resource type mismatch in {}".format(resource_name),
        )
        if isinstance(resource, DirectoryResourceBase):
            # Now we're only checking type
            return
        self.assertEqual(
            self.get_resource_sha256(resource),
            self.get_resource_sha256(expected),
            "Resource content mismatch in {}".format(resource_name),
        )

    def assertPayloadBagEqual(self, payload, expected, payload_bag_name):
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

    def assertTaskEqual(self, task, expected):
        self.assertDictEqual(task.headers, expected.headers, "Headers mismatch")
        self.assertPayloadBagEqual(task.payload, expected.payload, "payload")
        self.assertPayloadBagEqual(
            task.payload_persistent, expected.payload_persistent, "payload_persistent"
        )

    def assertTasksEqual(self, tasks, expected):
        """
        Checks whether task lists are equal
        :param tasks: Result tasks list
        :type tasks: List[:py:class:`karton.Task`]
        :param expected: Expected tasks list
        :type expected: List[:py:class:`karton.Task`]
        """
        self.assertEqual(len(tasks), len(expected), "Incorrect number of tasks sent")
        for task, other in zip(tasks, expected):
            self.assertTaskEqual(task, other)

    def run_task(self, task):
        """
        Spawns task into tested Karton subsystem instance
        :param task: Task to be spawned
        :type task: :py:class:`karton.Task`
        :return: Result tasks sent by Kartonik
        :rtype: List[:py:class:`karton.Task`]
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
    filters = []

    @classmethod
    def from_karton(cls, karton_class):
        """
        Turns Consumer into its mocked version, so we can test subsystem without interaction
        with infrastructure
        :param karton_class: Consumer-based Karton class
        :type karton_class: Type[Consumer]
        :return: Type[KartonMock]
        """
        if not issubclass(karton_class, Consumer):
            raise TypeError("Karton must be karton.Consumer subclass")

        if "internal_process" in karton_class.__dict__:
            raise TypeError("Karton can't override internal_process method")

        return type(karton_class.__name__ + "Mock", (cls,), dict(karton_class.__dict__))

    def __init__(self, config, **kwargs):
        self.config = config
        self.current_task = None
        self._result_tasks = []

    @property
    def minio(self):
        raise RuntimeError("Karton shouldn't interact directly with Minio")

    @property
    def rs(self):
        raise RuntimeError("Karton shouldn't interact directly with Redis")

    @property
    def log(self):
        return logging.getLogger(self.identity)

    def send_task(self, task):
        self.log.debug("Dispatched task %s", task.uid)

        # Complete information about task
        if self.current_task is not None:
            task.set_task_parent(self.current_task)
            task.merge_persistent_payload(self.current_task)
            task.priority = self.current_task.priority

        task.last_update = time.time()
        task.headers.update({"origin": self.identity})

        self._result_tasks.append(task)

    @contextlib.contextmanager
    def continue_asynchronic(self, task, finish=True):
        old_current_task = self.current_task
        self.current_task = task
        yield
        self.current_task = old_current_task

    def process(self):
        """
        Expected to be overwritten

        self.current_task contains task that triggered invocation of :py:meth:`karton.Consumer.process`
        """
        raise NotImplementedError()

    def run_task(self, task):
        """
        Provides task for processing and compares result tasks with expected ones

        :param task: Input task for subsystem
        """
        self._result_tasks = []
        self.current_task = task
        for bind in self.filters:
            if self.current_task.matches_bind(bind):
                break
        else:
            raise RuntimeError("Provided task doesn't match any of filters")
        self.process()
        return self._result_tasks
