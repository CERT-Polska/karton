import json

from minio import Minio
from redis import StrictRedis

from .logger import KartonLogHandler

OPERATIONS_QUEUE = "karton.operations"


class KartonBase(object):
    identity = ""

    def __init__(self, config, **kwargs):
        self.config = config
        self.rs = StrictRedis(decode_responses=True, **self.config.redis_config)

        self.current_task = None
        self.log_handler = KartonLogHandler(rs=self.rs)
        self.log = self.log_handler.get_logger(self.identity)

        self.minio = Minio(
            self.config.minio_config["address"],
            self.config.minio_config["access_key"],
            self.config.minio_config["secret_key"],
            secure=bool(int(self.config.minio_config.get("secure", True))),
        )

    def declare_task_state(self, task, status, identity=None):
        """
        Declare task state

        :param task: Task
        """
        self.rs.rpush(
            OPERATIONS_QUEUE,
            json.dumps(
                {
                    "status": status,
                    "identity": identity,
                    "task": task.serialize(),
                    "type": "operation",
                }
            ),
        )
