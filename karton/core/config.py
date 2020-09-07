import os

try:
    import configparser
except ImportError:
    import ConfigParser

    configparser = ConfigParser


class Config(object):
    """
    Simple config loader.

    Loads configuration from paths specified below (in provided order):

    - /etc/karton/karton.ini (global)
    - ~/.config/karton/karton.ini (user local)
    - ./karton.ini (subsystem local)

    :param path: path to additional configuration file
    """
    SEARCH_PATHS = [
        "/etc/karton/karton.ini",
        os.path.expanduser("~/.config/karton/karton.ini"),
        "./karton.ini",
    ]

    def __init__(self, path=None):
        if path is not None:
            if not os.path.isfile(path):
                raise IOError("Configuration file not found in " + path)
            self.SEARCH_PATHS = self.SEARCH_PATHS + [path]

        self.config = configparser.ConfigParser()
        if not self.config.read(self.SEARCH_PATHS):
            raise IOError("'karton.ini' configuration file not found")

        self.minio_config = dict(self.config.items("minio"))
        self.redis_config = dict(self.config.items("redis"))
