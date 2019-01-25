import configparser


class Config:
    def __init__(self, path):
        config = configparser.ConfigParser()
        config.read(path)

        self.minio_config = config["minio"]
        self.rmq_config = config["rmq"]
