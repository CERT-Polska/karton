"""
Base library for karton subsystems.
"""
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
        We send task as a child of a current one to maintain whole chain of events
        :param task:
        :return:
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

        # Mandatory tasks will fail if they're unroutable
        delivered = self.channel.basic_publish(TASKS_QUEUE, "", task_json, pika.BasicProperties(headers=task.headers),
                                               mandatory=True)

        if delivered:
            self.housekeeper.declare_task_state(task, TaskState.SPAWNED, identity=self.identity)
        else:
            self.log.debug("Task {} is unroutable".format(task.uid))
        return delivered


class Consumer(KartonBase):
    filters = None

    def __init__(self, config):
        super(Consumer, self).__init__(config=config)

        self.current_task = None
        self.killer = GracefulKiller(self.graceful_shutdown)

    def graceful_shutdown(self):
        self.log.info("Gracefully shutting down!")
        self.channel.stop_consuming()

    def process(self):
        raise RuntimeError("Not implemented.")

    def internal_process(self, channel, method, properties, body):
        self.current_task = Task.unserialize(properties.headers, body)
        self.log_handler.set_task(self.current_task)

        try:
            self.log.info("Received new task")
            self.housekeeper.declare_task_state(self.current_task, TaskState.STARTED, identity=self.identity)
            self.process()
            self.log.info("Task done")
        except Exception as e:
            self.log.exception("Failed to process task")
        finally:
            if not self.current_task.is_asynchronic():
                self.housekeeper.declare_task_state(self.current_task, TaskState.FINISHED, identity=self.identity)

    @RabbitMQClient.retryable
    def loop(self):
        self.log.info("Service {} started".format(self.identity))
        self.channel.queue_declare(queue=self.identity, durable=False, auto_delete=True)

        # RMQ in headers doesn't allow multiple filters, se we bind multiple times
        for filter in self.filters:
            filter.update({"x-match": "all"})
            self.channel.queue_bind(exchange=TASKS_QUEUE, queue=self.identity, routing_key='',
                                    arguments=filter)

        self.channel.basic_consume(self.internal_process, self.identity, no_ack=True)
        self.channel.start_consuming()

    def download_resource(self, resource):
        return resource.download(self.minio)

    def remove_resource(self, resource):
        return resource.remove(self.minio)

    def upload_resource(self, resource):
        return resource.upload(self.minio)


class Karton(Consumer, Producer):
    """
    This glues together Consumer and Producer - which is the most common use case
    """
