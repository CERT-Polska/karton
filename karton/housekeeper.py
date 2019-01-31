import json

import pika

from .logger import KartonLogHandler
from .rmq import RabbitMQClient
from .task import Task

OPERATIONS_QUEUE = "karton.operations"


class KartonHousekeeper(RabbitMQClient):
    identity = "housekeeper"

    def __init__(self, config=None, **kwargs):
        self.config = config
        self.task = None
        self.task_finished = None
        RabbitMQClient.__init__(self, **kwargs)
        self.log_handler = KartonLogHandler(connection=self.connection)
        self.log = self.log_handler.get_logger("karton." + self.identity)

    def process(self):
        raise RuntimeError("Not implemented.")

    def declare_task(self, task, finished=False):
        """
        Declare task state
        :param task: Task
        :param finished: Is task finished? (task is started if False)
        """
        self.channel.basic_publish(OPERATIONS_QUEUE, "", json.dumps({
            "finished": finished,
            "task": task.serialize()
        }), pika.BasicProperties(headers=task.headers))

    def internal_process(self, channel, method, properties, body):
        try:
            msg = json.loads(body)
            self.task_finished = msg["finished"]
            self.task = Task.unserialize(properties.headers,
                                         json.loads(msg["task"]),
                                         self.config.minio_config)
            self.process()
        except Exception as e:
            self.log.exception("Failed to process operation")

    @RabbitMQClient.retryable
    def loop(self):
        self.log.info("Housekeeper {} started".format(self.identity))
        self.channel.queue_declare(queue=self.identity, durable=False, auto_delete=True)

        # RMQ in headers doesn't allow multiple
        self.channel.queue_bind(exchange=OPERATIONS_QUEUE, queue=self.identity, routing_key='')

        self.channel.basic_consume(self.internal_process, queue=self.identity, no_ack=True)
        self.channel.start_consuming()
