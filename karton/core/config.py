import configparser
import itertools
import os
import re
import warnings
from typing import Any, Dict, List, Optional, overload


class ConfigSection:
    def __init__(self, config: "Config", section: str):
        self.config = config
        self.section = section

    def __getitem__(self, item: str) -> Any:
        return self.config.get(self.section, item)


def _format_env_parts(name: str) -> List[str]:
    name = name.upper()
    if re.fullmatch(r"[A-Z0-9_]+", name):
        return [name]
    parts = re.split(r"[^A-Z0-9]+", name.upper())
    return [name, "_".join(parts)]


def _combine_env_parts(*parts_list: List[str]) -> List[str]:
    # Filter out empty lists and make a product
    combinations = itertools.product(*[parts for parts in parts_list if parts])
    # Then produce list of environment value names
    return ["_".join(["KARTON", *combination]) for combination in combinations]


def get_env_names(section: str, option: Optional[str]) -> List[str]:
    if section.upper() == "KARTON":
        section_variants = []
    else:
        section_variants = _format_env_parts(section)
    option_variants = _format_env_parts(option) if option is not None else []
    return _combine_env_parts(section_variants, option_variants)


class Config:
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

    Environment variables have higher precedence than those loaded from files,
    but lower than arguments set via CLI.

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
        self._file_config: Dict[str, Dict[str, str]] = {}
        self._dict_config: Dict[str, Dict[str, Any]] = {}

        if path is not None:
            if not os.path.isfile(path):
                raise IOError("Configuration file not found in " + path)
            self.SEARCH_PATHS = self.SEARCH_PATHS + [path]

        self._load_from_file(self.SEARCH_PATHS)

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
        if self.has_section("minio"):
            config_fields = {
                "bucket": self.get("minio", "bucket"),
                "secure": self.getboolean("minio", "secure", fallback=True),
                "address": self.get("minio", "address"),
                "access_key": self.get("minio", "access_key"),
                "secret_key": self.get("minio", "secret_key"),
                "iam_auth": self.getboolean("s3", "iam_auth"),
            }
            if config_fields["address"] is not None and not (
                config_fields["address"].startswith("http://")
                or config_fields["address"].startswith("https://")
            ):
                scheme = "http://" if config_fields["secure"] else "https://"
                config_fields["address"] = scheme + config_fields["address"]
            for field, value in config_fields:
                if value is None:
                    continue
                self.set("s3", field, value)

    def set(self, section_name: str, option_name: str, value: Any) -> None:
        """
        Sets value in configuration
        """
        if section_name not in self._dict_config:
            self._dict_config[section_name] = {}
        self._dict_config[section_name][option_name] = value

    def get(
        self, section_name: str, option_name: str, fallback: Optional[Any] = None
    ) -> Any:
        """
        Gets value from configuration or returns ``fallback`` (None by default)
        if value was not set.
        """
        # Configuration sources must be looked up in a specific order
        if option_name in self._dict_config.get(section_name, {}):
            return self._dict_config[section_name][option_name]
        for env_name in get_env_names(section_name, option_name):
            if env_name in os.environ:
                return os.environ[env_name]
        if option_name in self._file_config.get(section_name, {}):
            return self._file_config[section_name][option_name]
        return fallback

    def has_section(self, section_name: str) -> bool:
        """
        Checks if configuration section exists

        Deprecated, may produce incorrect results when sections are provided
        via env variables and have common prefix.
        """
        if section_name in self._file_config:
            return True
        if section_name in self._dict_config:
            return True
        for environ_key in os.environ.keys():
            for section_prefix in get_env_names(section_name, option=None):
                if environ_key.startswith(section_prefix + "_"):
                    return True
        return False

    def has_option(self, section_name: str, option_name: str) -> bool:
        """
        Checks if configuration value is set
        """
        if option_name in self._file_config.get(section_name, {}):
            return True
        if option_name in self._dict_config.get(section_name, {}):
            return True
        for env_name in get_env_names(section_name, option_name):
            if env_name in os.environ:
                return True
        return False

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
        if self.has_option(section_name, option_name):
            existing_value = self.get(section_name, option_name)
            if isinstance(existing_value, list):
                raise TypeError(
                    f"{section_name}.{option_name} is "
                    f"{type(existing_value)} while "
                    f"list was expected"
                )
            self.set(section_name, option_name, existing_value + [value])
        else:
            self.set(section_name, option_name, [value])

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

        for section_name in config_file.sections():
            for option_name in config_file.options(section_name):
                option = config_file.get(section_name, option_name)
                if section_name not in self._file_config:
                    self._file_config[section_name] = {}
                self._file_config[section_name][option_name] = option

    def __getitem__(self, section) -> ConfigSection:
        """Gets a section named `section` from the config"""
        return ConfigSection(self, section)
