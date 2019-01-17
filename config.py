import configparser

config = configparser.ConfigParser()
config.read('config.ini')

minio_config = config["minio"]

