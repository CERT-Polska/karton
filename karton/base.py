from minio import Minio

from .logger import KartonLogHandler
from .rmq import RabbitMQClient, ExURLParameters


class KartonSimple(RabbitMQClient):
    identity = ""

    def __init__(self, config, **kwargs):
        self.config = config

        parameters = ExURLParameters(self.config.rmq_config["address"])
        super(KartonSimple, self).__init__(parameters=parameters, **kwargs)

        self.current_task = None
        self.log_handler = KartonLogHandler(connection=self.connection)
        self.log = self.log_handler.get_logger(self.identity)

        self.minio = Minio(self.config.minio_config["address"],
                           self.config.minio_config["access_key"],
                           self.config.minio_config["secret_key"],
                           secure=bool(int(self.config.minio_config.get("secure", True))))
