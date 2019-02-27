"""
Base library for karton subsystems.
"""
import pika

from .task import Task
from .resource import Resource, DirResource
from .rmq import RabbitMQClient, ExURLParameters
from .housekeeper import KartonHousekeeper, TaskState
from .logger import KartonLogHandler

TASKS_QUEUE = "karton.tasks"


class KartonBase(RabbitMQClient):
    identity = ""

    def __init__(self, config):
        self.config = config

        parameters = ExURLParameters(self.config.rmq_config["address"])
        super(KartonBase, self).__init__(parameters=parameters)

        self.current_task = None
        self.log_handler = KartonLogHandler(connection=self.connection)
        self.housekeeper = KartonHousekeeper(connection=self.connection)
        self.log = self.log_handler.get_logger(self.identity)


class Producer(KartonBase):
    def create_task(self, *args, **kwargs):
        return Task(*args, **kwargs)

    def create_resource(self, *args, **kwargs):
        if "bucket" not in kwargs:
            kwargs["bucket"] = self.config.minio_config["bucket"]
        return Resource(*args, config=self.config.minio_config, **kwargs)

    def create_dir_resource(self, *args, **kwargs):
        if "bucket" not in kwargs:
            kwargs["bucket"] = self.config.minio_config["bucket"]
        return DirResource(*args, config=self.config.minio_config, **kwargs)

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

        task_json = task.serialize()

        for resource in task.resources.values():
            resource.upload()

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

    def process(self):
        raise RuntimeError("Not implemented.")

    def internal_process(self, channel, method, properties, body):
        self.current_task = Task.unserialize(properties.headers, body, self.config.minio_config)
        self.log_handler.set_task(self.current_task)

        try:
            self.log.info("Received new task")
            self.housekeeper.declare_task_state(self.current_task, TaskState.STARTED, identity=self.identity)
            self.process()
            self.log.info("Task done")
        except Exception as e:
            self.log.exception("Failed to process task")
        finally:
            if not self.current_task.asynchronic:
                self.housekeeper.declare_task_state(self.current_task, TaskState.FINISHED, identity=self.identity)

    @RabbitMQClient.retryable
    def loop(self):
        self.log.info("Service {} started".format(self.identity))
        self.channel.queue_declare(queue=self.identity, durable=False, auto_delete=True)

        # RMQ in headers doesn't allow multiple
        for filter in self.filters:
            filter.update({"x-match": "all"})
            self.channel.queue_bind(exchange=TASKS_QUEUE, queue=self.identity, routing_key='',
                                    arguments=filter)

        self.channel.basic_consume(self.internal_process, self.identity, no_ack=True)
        self.channel.start_consuming()


class Karton(Consumer, Producer):
    """
    This glues together Consumer and Producer - which is the most common use case
    """
