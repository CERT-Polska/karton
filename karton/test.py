"""
Test stubs for karton subsystem unit tests
"""
import contextlib
import logging
import unittest
import time
import zipfile

from .karton import Consumer
from .resource import Resource, DirectoryResource

try:
    from unittest import mock
except ImportError:
    # Py2 compatibility: needs "mock" to be installed
    import mock

__all__ = ["KartonTestCase", "mock"]


class KartonTestCase(unittest.TestCase):
    karton_class = None
    config = None
    kwargs = None

    longMessage = True

    @classmethod
    def setUpClass(cls):
        cls._karton_mock = KartonMock.from_karton(cls.karton_class)

    def setUp(self):
        self.karton = self._karton_mock(self.config, **(self.kwargs or {}))

    def assertPayloadBagEqual(self, payload, other, payload_bag_name):
        self.assertSetEqual(
            set(payload.keys()),
            set(other.keys()),
            "Incorrect fields set in {}".format(payload_bag_name),
        )
        for key, value in payload.items():
            other_value = other[key]
            if not isinstance(value, Resource):
                self.assertEqual(
                    value,
                    other_value,
                    "Incorrect value of {}.{}".format(payload_bag_name, key),
                )
                continue
            self.assertTrue(
                isinstance(other_value, Resource),
                "Expected Resource type of {}.{}".format(payload_bag_name, key),
            )
            self.assertEqual(
                value.is_directory(),
                other_value.is_directory(),
                "Resource type mismatch in {}.{}".format(payload_bag_name, key),
            )
            self.assertEqual(
                value.sha256,
                other_value.sha256,
                "Resource content mismatch in {}.{}".format(payload_bag_name, key),
            )

    def assertTaskEqual(self, task, other):
        self.assertDictEqual(task.headers, other.headers, "Headers mismatch")
        self.assertPayloadBagEqual(task.payload, other.payload, "payload")
        self.assertPayloadBagEqual(
            task.payload_persistent, other.payload_persistent, "payload_persistent"
        )

    def assertTasksEqual(self, tasks, others):
        self.assertEqual(len(tasks), len(others), "Incorrect number of tasks sent")
        for task, other in zip(tasks, others):
            self.assertTaskEqual(task, other)

    def run_task(self, task):
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

    def download_resource(self, resource):
        if not isinstance(resource, Resource):
            raise TypeError("Local Resource was expected here")
        return resource

    @contextlib.contextmanager
    def download_to_temporary_folder(self, resource):
        if not resource.is_directory():
            raise TypeError(
                "Attempted to download resource that is NOT a directory as a directory."
            )
        if not isinstance(resource, DirectoryResource):
            raise TypeError("Local DirectoryResource was expected here")
        return resource.directory_path

    def download_zip_file(self, resource):
        if not resource.is_directory():
            raise TypeError(
                "Attempted to download resource that is NOT a directory as a directory."
            )
        return zipfile.ZipFile(resource.content)

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
