import configparser


class Config(object):
    def __init__(self, path):
        self.config = configparser.ConfigParser()
        self.config.read(path)

        self.minio_config = self.config["minio"]
        self.rmq_config = self.config["rmq"]
