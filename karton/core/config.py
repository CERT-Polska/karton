import configparser
import os
import re
from typing import Optional


class Config(object):
    """
    Simple config loader.

    Loads configuration from paths specified below (in provided order):

    - ``/etc/karton/karton.ini`` (global)
    - ``~/.config/karton/karton.ini`` (user local)
    - ``./karton.ini`` (subsystem local)

    It is also possible to pass configuration via environment variables.
    Any variable named KARTON_FOO_BAR is equivalent to setting 'bar' variable
    in section 'foo' (note the lowercase names).

    Environment variables have higher precedence than those loaded from files.

    :param path: Path to additional configuration file
    """

    SEARCH_PATHS = [
        "/etc/karton/karton.ini",
        os.path.expanduser("~/.config/karton/karton.ini"),
        "./karton.ini",
    ]

    def __init__(self, path: Optional[str] = None) -> None:
        if path is not None:
            if not os.path.isfile(path):
                raise IOError("Configuration file not found in " + path)
            self.SEARCH_PATHS = self.SEARCH_PATHS + [path]

        self.config = configparser.ConfigParser()
        self.config.read(self.SEARCH_PATHS)
        self._load_from_env()

        if not self.config.has_section("minio"):
            raise RuntimeError("Missing MinIO configuration")
        if not self.config.has_section("redis"):
            raise RuntimeError("Missing Redis configuration")

    def _load_from_env(self):
        """Function used for loading configuration items from the environment variables

        :meta private:
        """

        for name, value in os.environ.items():
            # Load env variables named KARTON_[section]_[key]
            # to match ConfigParser structure
            result = re.fullmatch(r"KARTON_([A-Z0-9]+)_([A-Z0-9_]+)", name)

            if not result:
                continue

            section, key = result.groups()
            section = section.lower()
            key = key.lower()

            if not self.config.has_section(section):
                self.config[section] = {}

            self.config[section][key] = value

    def __getitem__(self, section):
        """ Gets a section named `section` from the config """
        return self.config[section]

    @property
    def minio_config(self):
        """ Compat """
        return dict(self.config["minio"])

    @property
    def redis_config(self):
        """ Compat """
        return dict(self.config["redis"])
