"""
Base library for karton housekeeper listeners.
"""
import json

from enum import Enum
import pika

from karton.base import KartonSimple

OPERATIONS_QUEUE = "karton.operations"


class TaskState(str, Enum):
    SPAWNED = "Spawned"
    STARTED = "Started"
    FINISHED = "Finished"


class KartonHousekeeper(KartonSimple):
    identity = "housekeeper"

    def declare_task_state(self, task, status, identity=None):
        """
        Declare task state
        :param task: Task
        :param finished: Is task finished? (task is started if False)
        """
        self.channel.basic_publish(OPERATIONS_QUEUE, "", json.dumps({
            "status": status,
            "identity": identity,
            "task": task.serialize(),
            "type": "operation"
        }), pika.BasicProperties(headers=task.headers))

