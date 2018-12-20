"""
Base library for karton subsystems.
"""

import json
import traceback
import uuid

import pika
import logging


class RabbitMQHandler(logging.Handler):
    def __init__(self, parameters):
        logging.Handler.__init__(self)
        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()

    def emit(self, record: logging.LogRecord):
        ignore_fields = ["args", "asctime", "msecs", "msg", "pathname", "process", "processName", "relativeCreated",
                         "exc_info", "exc_text", "stack_info", "thread", "threadName"]
        log_line = {k: v for k, v in record.__dict__.items() if k not in ignore_fields}
        if record.exc_info:
            log_line["excText"] = logging.Formatter().formatException(record.exc_info)
            log_line["excValue"] = str(record.exc_info[1])
            log_line["excType"] = record.exc_info[0].__name__
            log_line["excTraceback"] = traceback.format_exception(*record.exc_info)

        try:
            self.channel.basic_publish("karton.logs", "", json.dumps(log_line), pika.BasicProperties())
        except pika.exceptions.ChannelClosed:
            pass

    def close(self):
        self.connection.close()


class KartonBaseService:
    identity = None

    def __init__(self, parameters):
        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()
        self.current_task = None

        self.log = logging.getLogger(self.identity)
        self.log.setLevel(logging.DEBUG)
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(logging.Formatter("[%(asctime)s][%(levelname)s] %(message)s"))
        self.log.addHandler(stream_handler)
        self.log.addHandler(RabbitMQHandler(parameters))

    def process(self):
        raise RuntimeError("Not implemented.")

    def send_create_task(self, task):
        if self.current_task:
            parent_task_id = self.current_task.uid
        else:
            parent_task_id = None

        op_json = json.dumps({
            "type": "create_task",
            "event": {
                "identity": self.identity,
                "parent_task_id": parent_task_id,
                "task_id": task.uid
            }
        })

        self.channel.basic_publish("karton.operations", "", op_json, pika.BasicProperties())

    def send_task(self, task):
        self.send_create_task(task)
        task_json = task.serialize()
        self.channel.basic_publish("karton.tasks", "", task_json, pika.BasicProperties(headers=task.headers))

    def internal_process(self, channel, method, properties, body):
        msg = json.loads(body)
        self.current_task = Task.unserialize(properties.headers, msg)

        try:
            self.process()
        except Exception as e:
            self.log.exception("Failed to process task")

    def loop(self):
        self.channel.basic_consume(self.internal_process, queue=self.identity, no_ack=True)
        self.channel.start_consuming()

    def close(self):
        self.connection.close()


class Task:
    def __init__(self, headers, resources, payload):
        self.uid = str(uuid.uuid4())

        self.headers = headers
        self.resources = resources
        self.payload = payload

    def serialize(self):
        return json.dumps({"uid": self.uid, "resources": self.resources, "payload": self.payload})

    @staticmethod
    def unserialize(headers, data):
        task = Task(headers, data["resources"], data["payload"])
        task.uid = data["uid"]
        return task

    def __repr__(self):
        return self.serialize()
