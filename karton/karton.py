"""
Base library for karton subsystems.
"""
import json
import traceback

import pika
import logging

from .task import Task
from .resource import Resource, DirResource
from .rmq import RabbitMQClient

TASKS_QUEUE = "karton.tasks"
LOGS_QUEUE = "karton.logs"


class RabbitMQHandler(logging.Handler, RabbitMQClient):
    def __init__(self, connection):
        logging.Handler.__init__(self)
        RabbitMQClient.__init__(self, connection=connection)
        self.task_id = 'unknown'

    def set_task_id(self, task_id):
        self.task_id = task_id

    @RabbitMQClient.retryable
    def emit(self, record):
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

        self.channel.basic_publish(LOGS_QUEUE, "", json.dumps(log_line), pika.BasicProperties())


class Karton(RabbitMQClient):
    identity = ""
    filters = None

    def __init__(self, config):
        self.config = config

        parameters = pika.URLParameters(self.config.rmq_config["address"])
        super(Karton, self).__init__(parameters=parameters)

        self.current_task = None

        self.log = logging.getLogger("karton."+self.identity)
        self.log.setLevel(logging.DEBUG)
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(logging.Formatter("[%(asctime)s][%(levelname)s] %(message)s"))
        self.rmq_handler = RabbitMQHandler(self.connection)
        self.log.addHandler(stream_handler)
        self.log.addHandler(self.rmq_handler)

    def process(self):
        raise RuntimeError("Not implemented.")

    def create_task(self, *args, **kwargs):
        return Task(*args, **kwargs)

    def create_resource(self, *args, **kwargs):
        return Resource(*args, bucket=self.config.minio_config["bucket"], config=self.config.minio_config, **kwargs)

    def create_dir_resource(self, *args, **kwargs):
        return DirResource(*args, bucket=self.config.minio_config["bucket"], config=self.config.minio_config, **kwargs)

    @RabbitMQClient.retryable
    def send_task(self, task):
        """
        We send task as a child of a current one to maintain whole chain of events
        :param task:
        :return:
        """
        if self.current_task is not None:
            task.bind_task(self.current_task)

        task_json = task.serialize()
        for resource in task.resources:
            resource._upload()

        self.channel.basic_publish(TASKS_QUEUE, "", task_json, pika.BasicProperties(headers=task.headers))

    def internal_process(self, channel, method, properties, body):
        msg = json.loads(body)
        self.current_task = Task.unserialize(properties.headers, msg, self.config.minio_config)
        self.rmq_handler.set_task_id(self.current_task.uid)

        try:
            self.log.info("Received new task")
            self.process()
            self.log.info("Task done")
        except Exception as e:
            self.log.exception("Failed to process task")

    @RabbitMQClient.retryable
    def loop(self):
        self.log.info("Service {} started".format(self.identity))
        self.channel.queue_declare(queue=self.identity, durable=False, auto_delete=True)

        # RMQ in headers doesn't allow multiple
        for filter in self.filters:
            filter.update({"x-match": "all"})
            self.channel.queue_bind(exchange=TASKS_QUEUE, queue=self.identity, routing_key='',
                                    arguments=filter)

        self.channel.basic_consume(self.internal_process, queue=self.identity, no_ack=True)
        self.channel.start_consuming()
