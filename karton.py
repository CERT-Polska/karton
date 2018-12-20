import json

import pika
import logging

from log import RabbitMQHandler


class KartonBaseService:
    identity = None

    def __init__(self):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
        self.channel = connection.channel()
        self.log = logging.getLogger(self.identity)
        self.log.setLevel(logging.DEBUG)
        self.log.addHandler(logging.StreamHandler())
        self.log.addHandler(RabbitMQHandler())

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
    def __init__(self, headers, resources):
        self.resources = resources
        self.headers = headers

    def serialize(self):
        return json.dumps({"resources": self.resources, "headers": self.headers})

    def __repr__(self):
        return self.serialize()
