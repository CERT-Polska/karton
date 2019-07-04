"""
Base library for karton subsystems.
"""
import contextlib

import pika

from .base import KartonSimple
from .resource import RemoteResource, RemoteDirectoryResource
from .task import Task
from .rmq import RabbitMQClient
from .housekeeper import KartonHousekeeper, TaskState
from .utils import GracefulKiller

TASKS_QUEUE = "karton.tasks"


class KartonBase(KartonSimple):
    identity = ""

    def __init__(self, config, **kwargs):
        super(KartonBase, self).__init__(config=config, **kwargs)
        self.housekeeper = KartonHousekeeper(config=config, connection=self.connection)


class Producer(KartonBase):
    @RabbitMQClient.retryable
    def send_task(self, task):
        """
        Sends a task to the RabbitMQ queue. Takes care of logging.
        Given task will be child of task we are currently handling (if such exists) - this ensures our log continuity

        :param task: task to be sent
        :type task: :py:class:`karton.Task`
        :rtype: bool
        :return: if task was delivered
        """
        self.log.debug("Dispatched task {}".format(task.uid))
        if self.current_task is not None:
            task.set_task_parent(self.current_task)

        for name, resource in task.payload.resources():
            if type(resource) is not RemoteResource and type(resource) is not RemoteDirectoryResource:
                task.payload[name] = resource.upload(self.minio, self.config.minio_config["bucket"])

        task_json = task.serialize()

        # Enables delivery confirmation
        self.channel.confirm_delivery()

        headers = task.headers.copy()
        headers.update({"origin": self.identity})

        # Mandatory tasks will fail if they're unroutable
        delivered = self.channel.basic_publish(TASKS_QUEUE, "", task_json, pika.BasicProperties(headers=headers),
                                               mandatory=True)

        if delivered:
            self.housekeeper.declare_task_state(task, TaskState.SPAWNED, identity=self.identity)
        else:
            self.log.debug("Task {} is unroutable".format(task.uid))
        return delivered


class Consumer(KartonBase):
    """
    Base consumer class, expected to be inherited from

    Usage example:

        .. code-block:: python

            from karton import Consumer

            class Reporter(Consumer):
                identity = "karton.reporter"
                filters = [
                    {
                        "type": "sample",
                        "stage": "recognized"
                    },
                    {
                        "type": "config"
                    }
                ]
                def process():
                    # handle self.current_task, eg.
                    if self.current_task.headers["type"] == "sample":
                       return self.process_sample()
                    else:
                       return self.process_config()


    """
    filters = None

    def __init__(self, config):
        super(Consumer, self).__init__(config=config)

        self.current_task = None
        self.killer = GracefulKiller(self.graceful_shutdown)

    def graceful_shutdown(self):
        self.log.info("Gracefully shutting down!")
        self.channel.stop_consuming()
        self.shutdown()

    def process(self):
        """
        Expected to be overwritten

        self.current_task contains task that triggered invocation of :py:meth:`karton.Consumer.process`
        """
        raise RuntimeError("Not implemented.")

    def internal_process(self, channel, method, properties, body):
        self.current_task = Task.unserialize(properties.headers, body)
        self.log_handler.set_task(self.current_task)

        try:
            self.log.info("Received new task - {}".format(self.current_task.uid))
            self.housekeeper.declare_task_state(self.current_task, TaskState.STARTED, identity=self.identity)
            self.process()
            self.log.info("Task done - {}".format(self.current_task.uid))
        except Exception:
            self.log.exception("Failed to process task - {}".format(self.current_task.uid))
        finally:
            if not self.current_task.is_asynchronic():
                self.housekeeper.declare_task_state(self.current_task, TaskState.FINISHED, identity=self.identity)

    @RabbitMQClient.retryable
    def loop(self):
        """
        Blocking loop that consumes tasks and runs :py:meth:`karton.Consumer.process` as a handler
        """
        self.log.info("Service {} started".format(self.identity))
        self.channel.queue_declare(queue=self.identity, durable=False, auto_delete=True)

        # RMQ in headers doesn't allow multiple filters, se we bind multiple times
        for filter in self.filters:
            self.log.info("Binding on: {}".format(filter))
            filter.update({"x-match": "all"})
            self.channel.queue_bind(exchange=TASKS_QUEUE, queue=self.identity, routing_key='',
                                    arguments=filter)

        self.channel.basic_consume(self.internal_process, self.identity, no_ack=True)
        self.channel.start_consuming()

    def download_resource(self, resource):
        """
        Download remote resource into local resource.

        :param resource: resource to download
        :type resource: :py:class:`karton.RemoteResource`
        :rtype: :py:class:`karton.Resource`
        :return: downloaded resource
        """
        return resource.download(self.minio)

    @contextlib.contextmanager
    def download_to_temporary_folder(self, resource):
        """
        Context manager for downloading remote directory resource into local temporary folder.
        It also makes sure that the temporary folder is disposed afterwards.

        :param resource: resource to download
        :type resource: :py:class:`karton.RemoteDirectoryResource`
        :raises: TypeError
        :rtype: str
        :return: path to temporary folder with unpacked contents
        """
        if not resource.is_directory():
            raise TypeError("Attempted to download resource that is NOT a directory as a directory.")
        with resource.download_to_temporary_folder(self.minio) as fpath:
            yield fpath

    def download_zip_file(self, resource):
        """
        Download remote directory resource contents into Zipfile object.

        :param resource: resource to download
        :type resource: :py:class:`karton.RemoteDirectoryResource`
        :rtype: :py:class:`zipfile.Zipfile`
        :return: zipfile with downloaded contents
        """
        if not resource.is_directory():
            raise TypeError("Attempted to download resource that is NOT a directory as a directory.")
        return resource.download_zip_file(self.minio)

    def remove_resource(self, resource):
        """
        Remove remote resource.

        :param resource: resource to be removed
        :type resource: :py:class:`karton.RemoteResource`
        """
        return resource.remove(self.minio)

    def upload_resource(self, resource):
        """
        Upload local resource to the storage hub

        :param resource: resource to upload
        :type resource: :py:class:`karton.Resource`
        :rtype: :py:class:`karton.RemoteResource`
        :return: representing uploaded resource
        """
        return resource.upload(self.minio)


class Karton(Consumer, Producer):
    """
    This glues together Consumer and Producer - which is the most common use case
    """
