try:
    import configparser
except ImportError:
    import ConfigParser
    configparser = ConfigParser


class Config(object):
    def __init__(self, path):
        self.config = configparser.ConfigParser()
        self.config.read(path)

        self.minio_config = dict(self.config.items("minio"))
        self.rmq_config = dict(self.config.items("rmq"))
