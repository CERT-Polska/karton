"""
Base library for karton subsystems.
"""

import json
import traceback
from typing import List

import pika
import logging


class RabbitMQHandler(logging.Handler):
    def __init__(self, parameters: pika.ConnectionParameters):
        logging.Handler.__init__(self)
        connection = pika.BlockingConnection(parameters)
        self.channel = connection.channel()

    def emit(self, record: logging.LogRecord):
        ignore_fields = ["args", "asctime", "msecs", "msg", "pathname", "process", "processName", "relativeCreated",
                         "exc_info", "exc_text", "stack_info", "thread", "threadName"]
        log_line = {k: v for k, v in record.__dict__.items() if k not in ignore_fields}
        if record.exc_info:
            log_line["excText"] = logging.Formatter().formatException(record.exc_info)
            log_line["excValue"] = str(record.exc_info[1])
            log_line["excType"] = record.exc_info[0].__name__
            log_line["excTraceback"] = traceback.format_exception(*record.exc_info)

        self.channel.basic_publish("karton.logs", "", json.dumps(log_line), pika.BasicProperties())

    def close(self):
        self.channel.close()


class KartonBaseService:
    identity = None

    def __init__(self, parameters):
        connection = pika.BlockingConnection(parameters)
        self.channel = connection.channel()
        self.log = logging.getLogger(self.identity)
        self.log.setLevel(logging.DEBUG)
        self.log.addHandler(logging.StreamHandler())
        self.log.addHandler(RabbitMQHandler(parameters))

    def process(self, task):
        raise RuntimeError("Not implemented.")

    def send_task(self, task):
        task_json = task.serialize()
        self.channel.basic_publish("karton.tasks", "", task_json, pika.BasicProperties(headers=task.headers))

    def internal_process(self, channel, method, properties, body):
        msg = json.loads(body)
        task = Task(properties.headers, msg["resources"])
        try:
            self.process(task)
        except Exception as e:
            self.log.exception("Failed to process task")

    def loop(self):
        self.channel.basic_consume(self.internal_process, queue=self.identity, no_ack=True)
        self.channel.start_consuming()


class Task:
    def __init__(self, headers: dict, resources: List[str]):
        self.resources = resources
        self.headers = headers

    def serialize(self):
        return json.dumps({"resources": self.resources, "headers": self.headers})

    def __repr__(self):
        return self.serialize()
