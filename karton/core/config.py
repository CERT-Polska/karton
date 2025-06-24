import configparser
import os
import re
import warnings
from typing import Any, Dict, List, Optional, cast, overload


class Config(object):
    """
    Simple config loader.

    Loads configuration from paths specified below (in provided order):

    - ``/etc/karton/karton.ini`` (global)
    - ``~/.config/karton/karton.ini`` (user local)
    - ``./karton.ini`` (subsystem local)
    - ``<path>`` optional, additional path provided in arguments

    It is also possible to pass configuration via environment variables.
    Any variable named KARTON_FOO_BAR is equivalent to setting 'bar' variable
    in section 'foo' (note the lowercase names).

    Environment variables have higher precedence than those loaded from files.

    :param path: Path to additional configuration file
    :param check_sections: Check if sections ``redis`` and ``s3`` are defined
        in the configuration
    """

    SEARCH_PATHS = [
        "/etc/karton/karton.ini",
        os.path.expanduser("~/.config/karton/karton.ini"),
        "./karton.ini",
    ]

    def __init__(
        self, path: Optional[str] = None, check_sections: Optional[bool] = True
    ) -> None:
        self._config: Dict[str, Dict[str, Any]] = {}

        if path is not None:
            if not os.path.isfile(path):
                raise IOError("Configuration file not found in " + path)
            self.SEARCH_PATHS = self.SEARCH_PATHS + [path]

        self._load_from_file(self.SEARCH_PATHS)
        self._load_from_env()

        if check_sections:
            if self.has_section("minio") and not self.has_section("s3"):
                self._map_minio_to_s3()
            if not self.has_section("s3"):
                raise RuntimeError("Missing S3 configuration")
            if not self.has_section("redis"):
                raise RuntimeError("Missing Redis configuration")

    def _map_minio_to_s3(self):
        """
        Configuration backwards compatibility. Before 5.x.x [minio] section was used.
        """
        warnings.warn(
            "[minio] section in configuration is deprecated, replace it with [s3]"
        )
        self._config["s3"] = dict(self._config["minio"])
        if not (
            self._config["s3"]["address"].startswith("http://")
            or self._config["s3"]["address"].startswith("https://")
        ):
            if self.getboolean("minio", "secure", True):
                self._config["s3"]["address"] = (
                    "https://" + self._config["s3"]["address"]
                )
            else:
                self._config["s3"]["address"] = (
                    "http://" + self._config["s3"]["address"]
                )

    def set(self, section_name: str, option_name: str, value: Any) -> None:
        """
        Sets value in configuration
        """
        if section_name not in self._config:
            self._config[section_name] = {}
        self._config[section_name][option_name] = value

    def get(
        self, section_name: str, option_name: str, fallback: Optional[Any] = None
    ) -> Any:
        """
        Gets value from configuration or returns ``fallback`` (None by default)
        if value was not set.
        """
        if not self.has_option(section_name, option_name):
            return fallback
        return self._config[section_name][option_name]

    def has_section(self, section_name: str) -> bool:
        """
        Checks if configuration section exists
        """
        return section_name in self._config

    def has_option(self, section_name: str, option_name: str) -> bool:
        """
        Checks if configuration value is set
        """
        if section_name not in self._config:
            return False
        if option_name not in self._config[section_name]:
            return False
        return True

    @overload
    def getint(self, section_name: str, option_name: str, fallback: int) -> int: ...

    @overload
    def getint(self, section_name: str, option_name: str) -> Optional[int]: ...

    @overload
    def getint(
        self, section_name: str, option_name: str, fallback: Optional[int]
    ) -> Optional[int]: ...

    def getint(
        self, section_name: str, option_name: str, fallback: Optional[int] = None
    ) -> Optional[int]:
        """
        Gets value from configuration or returns ``fallback`` (None by default)
        if value was not set. Value is coerced to int type.
        """
        value = self.get(section_name, option_name, fallback)
        if value is None:
            return None
        return int(value)

    @overload
    def getboolean(
        self, section_name: str, option_name: str, fallback: bool
    ) -> bool: ...

    @overload
    def getboolean(self, section_name: str, option_name: str) -> Optional[bool]: ...

    def getboolean(
        self, section_name: str, option_name: str, fallback: Optional[bool] = None
    ) -> Optional[bool]:
        """
        Gets value from configuration or returns ``fallback`` (None by default)
        if value was not set. Value is coerced to bool type.

        .. seealso::

           https://docs.python.org/3/library/configparser.html#configparser.ConfigParser.getboolean
        """
        value = self.get(section_name, option_name, fallback)
        if value is None:
            return None
        if type(value) is bool:
            return value
        if type(value) is str and value.lower() in ["1", "yes", "true", "on"]:
            return True
        elif type(value) is str and value.lower() in ["0", "no", "false", "off"]:
            return False
        else:
            raise ValueError(f"{section_name}.{option_name} is not a correct boolean")

    def append_to_list(self, section_name: str, option_name: str, value: Any) -> None:
        """
        Appends value to a list in configuration
        """
        if section_name not in self._config:
            self._config[section_name] = {}
        if option_name not in self._config[section_name]:
            self._config[section_name][option_name] = []
        elif not isinstance(self._config[section_name][option_name], list):
            raise TypeError(
                f"{section_name}.{option_name} is "
                f"{type(self._config[section_name][option_name])} while "
                f"list was expected"
            )
        self._config[section_name][option_name].append(value)

    def load_from_dict(self, data: Dict[str, Dict[str, Any]]) -> None:
        """
        Updates configuration values from dictionary compatible with
        ``ConfigParser.read_dict``. Accepts value in native type, so you
        don't need to convert them to string.

        None values are treated like missing value and are not added.

        .. code-block:: json

            {
               "section-name": {
                   "option-name": "value"
               }
            }

        """
        for section_name, section in data.items():
            for option_name, value in section.items():
                if value is None:
                    continue
                self.set(section_name, option_name, value)

    def _load_from_file(self, paths: List[str]) -> None:
        """
        Function used for loading configuration items from karton.ini files

        :meta private:
        """
        config_file = configparser.ConfigParser()
        config_file.read(paths)
        self.load_from_dict(cast(Dict[str, Dict[str, Any]], config_file))

    def _load_from_env(self) -> None:
        """
        Function used for loading configuration items from the environment variables

        :meta private:
        """
        for name, value in os.environ.items():
            # Load env variables named KARTON_[section]_[key]
            # to match ConfigParser structure
            result = re.fullmatch(r"KARTON_([A-Z0-9-]+)_([A-Z0-9_]+)", name)

            if not result:
                continue

            section, key = result.groups()
            section = section.lower()
            key = key.lower()
            self.set(section, key, value)

    def __getitem__(self, section) -> Dict[str, Any]:
        """Gets a section named `section` from the config"""
        return self._config[section]
