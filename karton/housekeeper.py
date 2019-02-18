"""
Base library for karton housekeeper listeners.
"""
import json

from enum import Enum
import pika

from .logger import KartonLogHandler
from .rmq import RabbitMQClient

OPERATIONS_QUEUE = "karton.operations"


class TaskState(str, Enum):
    SPAWNED = "Spawned"
    STARTED = "Started"
    FINISHED = "Finished"


class KartonHousekeeper(RabbitMQClient):
    identity = "housekeeper"

    def __init__(self, config=None, **kwargs):
        self.config = config
        self.message = {}
        RabbitMQClient.__init__(self, **kwargs)
        self.log_handler = KartonLogHandler(connection=self.connection)
        self.log = self.log_handler.get_logger("karton." + self.identity)

    def process(self):
        raise NotImplementedError()

    def declare_task_state(self, task, status, identity=None):
        """
        Declare task state
        :param task: Task
        :param finished: Is task finished? (task is started if False)
        """
        self.channel.basic_publish(OPERATIONS_QUEUE, "", json.dumps({
            "status": status,
            "identity": identity,
            "task": task.serialize()
        }), pika.BasicProperties(headers=task.headers))

    def internal_process(self, channel, method, properties, body):
        raise NotImplementedError()

    def loop(self):
        raise NotImplementedError()
