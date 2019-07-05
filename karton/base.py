from minio import Minio

from .logger import KartonLogHandler
from .rmq import RabbitMQClient
from pika import ConnectionParameters
from pika.credentials import ExternalCredentials
import os
import ssl


class KartonSimple(RabbitMQClient):
    identity = ""

    def __init__(self, config, **kwargs):
        self.config = config
        paths = self.config.rmq_config["ca_certs"], \
            self.config.rmq_config["keyfile"], \
            self.config.rmq_config["certfile"]

        for path in paths:
            if not os.path.isabs(path):
                raise ValueError("Certificate paths must be absolute")

        parameters = ConnectionParameters(host=self.config.rmq_config["host"],
                                          virtual_host=self.config.rmq_config["vhost"],
                                          credentials=ExternalCredentials(),
                                          connection_attempts=5,
                                          ssl=True,
                                          ssl_options=dict(
                                              ssl_version=ssl.PROTOCOL_TLSv1,
                                              ca_certs=self.config.rmq_config["ca_certs"],
                                              keyfile=self.config.rmq_config["keyfile"],
                                              certfile=self.config.rmq_config["certfile"],
                                              cert_reqs=ssl.CERT_REQUIRED
                                          ))
        super(KartonSimple, self).__init__(parameters=parameters, **kwargs)

        self.current_task = None
        self.log_handler = KartonLogHandler(connection=self.connection)
        self.log = self.log_handler.get_logger(self.identity)

        self.minio = Minio(self.config.minio_config["address"],
                           self.config.minio_config["access_key"],
                           self.config.minio_config["secret_key"],
                           secure=bool(int(self.config.minio_config.get("secure", True))))
