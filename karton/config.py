try:
    import configparser
except ImportError:
    import ConfigParser

    configparser = ConfigParser


class Config(object):
    def __init__(self, path):
        """
        Simple config loader, minio and RabbitMQ sections are required, for more information look at config.ini.example

        :param path: path to config.ini
        """
        self.config = configparser.ConfigParser()
        self.config.read(path)

        self.minio_config = dict(self.config.items("minio"))
        self.redis_config = dict(self.config.items("redis"))
