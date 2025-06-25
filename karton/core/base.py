import abc
import argparse
import logging
import os
import textwrap
from contextlib import contextmanager
from typing import Optional, Union, cast

from .__version__ import __version__
from .backend import KartonBackend, KartonServiceInfo
from .config import Config
from .logger import KartonLogHandler, TaskContextFilter
from .task import Task, get_current_task, set_current_task
from .utils import HardShutdownInterrupt, StrictClassMethod, graceful_killer


class ConfigMixin:
    identity: Optional[str]
    version: Optional[str]

    def __init__(self, config: Optional[Config] = None, identity: Optional[str] = None):
        self.config = config or Config()
        self.enable_publish_log = self.config.getboolean(
            "logging", "enable_publish", True
        )

        # If not passed via constructor - get it from class
        if identity is not None:
            self.identity = identity

        # If passed via configuration: override
        if self.config.has_option("karton", "identity"):
            self.identity = self.config.get("karton", "identity")

        self.debug = self.config.getboolean("karton", "debug", False)

        if self.debug and self.identity:
            self.identity += "-" + os.urandom(4).hex() + "-dev"

    @classmethod
    def args_description(cls) -> str:
        """Return short description for argument parser."""
        if not cls.__doc__:
            return ""
        return textwrap.dedent(cls.__doc__).strip().splitlines()[0]

    @classmethod
    def args_parser(cls) -> argparse.ArgumentParser:
        """
        Return ArgumentParser for main() class method.

        This method should be overridden and call super methods
        if you want to add more arguments.
        """
        parser = argparse.ArgumentParser(description=cls.args_description())
        parser.add_argument(
            "--version", action="version", version=cast(str, cls.version)
        )
        parser.add_argument("--config-file", help="Alternative configuration path")
        parser.add_argument(
            "--identity", help="Alternative identity for Karton service"
        )
        parser.add_argument("--log-level", help="Logging level of Karton logger")
        parser.add_argument(
            "--debug", help="Enable debugging mode", action="store_true", default=None
        )
        return parser

    @classmethod
    def config_from_args(cls, config: Config, args: argparse.Namespace) -> None:
        """
        Updates configuration with settings from arguments

        This method should be overridden and call super methods
        if you want to add more arguments.
        """
        config.load_from_dict(
            {
                "karton": {
                    "identity": args.identity,
                    "debug": args.debug,
                },
                "logging": {"level": args.log_level},
            }
        )

    @classmethod
    def karton_from_args(cls, args: Optional[argparse.Namespace] = None):
        """
        Returns Karton instance configured using configuration files
        and provided arguments

        Used by :py:meth:`KartonServiceBase.main` method
        """
        if args is None:
            parser = cls.args_parser()
            args = parser.parse_args()
        config = Config(path=args.config_file)
        cls.config_from_args(config, args)
        return cls(config=config)


class LoggingMixin:
    config: Config
    identity: Optional[str]
    debug: bool
    enable_publish_log: bool

    def __init__(self, log_handler: logging.Handler, log_format: str) -> None:
        self._log_handler = log_handler
        self._log_format = log_format

    def setup_logger(self, level: Optional[Union[str, int]] = None) -> None:
        """
        Setup logger for Karton service (StreamHandler and `karton.logs` handler)

        Called by :py:meth:`Consumer.loop`. If you want to use logger for Producer,
        you need to call it yourself, but remember to set the identity.

        :param level: Logging level. Default is logging.INFO \
                      (unless different value is set in Karton config)
        """
        if level is None:
            level = self.config.get("logging", "level", logging.INFO)

        if type(level) is str and cast(str, level).isdigit():
            log_level: Union[str, int] = int(level)
        else:
            log_level = level

        if not self.identity:
            raise ValueError("Can't setup logger without identity")

        self._log_handler.setFormatter(logging.Formatter())

        logger = logging.getLogger(self.identity)
        logger.addFilter(TaskContextFilter())

        if logger.handlers:
            # If logger already have handlers set: clear them
            logger.handlers.clear()

        # Turn off propagation to parent loggers to avoid double logging
        # We set up StreamHandler directly on logger, so it may be needed
        # in case basicConfig was called at some point.
        logger.propagate = False

        logger.setLevel(log_level)
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(logging.Formatter(self._log_format))
        logger.addHandler(stream_handler)

        if not self.debug and self.enable_publish_log:
            logger.addHandler(self._log_handler)

    @property
    def log_handler(self) -> logging.Handler:
        """
        Return KartonLogHandler bound to this Karton service.

        Can be used to setup logging on your own by adding this handler
        to the chosen loggers.
        """
        return self._log_handler

    @property
    def log(self) -> logging.Logger:
        """
        Return Logger instance for Karton service

        If you want to use it in code that is outside of the Consumer class,
        use :func:`logging.getLogger`:

        .. code-block:: python

            import logging
            logging.getLogger("<identity>")

        :return: :py:meth:`Logging.Logger` instance
        """
        return logging.getLogger(self.identity)


class KartonBase(abc.ABC, ConfigMixin, LoggingMixin):
    """
    Base class for all Karton services

    You can set an informative version information by setting the ``version`` class
    attribute.
    """

    #: Karton service identity
    identity: str = ""
    #: Karton service version
    version: Optional[str] = None
    #: Include extended service information for non-consumer services
    with_service_info: bool = False

    def __init__(
        self,
        config: Optional[Config] = None,
        identity: Optional[str] = None,
        backend: Optional[KartonBackend] = None,
    ) -> None:
        ConfigMixin.__init__(self, config, identity)

        self.service_info = None
        if self.identity is not None and self.with_service_info:
            self.service_info = KartonServiceInfo(
                identity=self.identity,
                karton_version=__version__,
                service_version=self.version,
            )

        self.backend = backend or KartonBackend(
            self.config, identity=self.identity, service_info=self.service_info
        )

        log_handler = KartonLogHandler(backend=self.backend, channel=self.identity)
        LoggingMixin.__init__(
            self, log_handler, log_format="[%(asctime)s][%(levelname)s] %(message)s"
        )

    @property
    def current_task(self) -> Optional[Task]:
        return get_current_task()

    @current_task.setter
    def current_task(self, task: Optional[Task]):
        set_current_task(task)


class KartonServiceBase(KartonBase):
    """
    Karton base class for looping services.

    You can set an informative version information by setting the ``version`` class
    attribute

    :param config: Karton config to use for service configuration
    :param identity: Karton service identity to use
    :param backend: Karton backend to use
    """

    def __init__(
        self,
        config: Optional[Config] = None,
        identity: Optional[str] = None,
        backend: Optional[KartonBackend] = None,
    ) -> None:
        super().__init__(
            config=config,
            identity=identity,
            backend=backend,
        )
        self.setup_logger()
        self._shutdown = False

    def _do_shutdown(self) -> None:
        self.log.info("Got signal, shutting down...")
        self._shutdown = True

    @property
    def shutdown(self):
        return self._shutdown

    @contextmanager
    def graceful_killer(self):
        try:
            with graceful_killer(self._do_shutdown):
                yield
        except HardShutdownInterrupt:
            self.log.info("Hard shutting down!")
            raise

    # Base class for Karton services
    @abc.abstractmethod
    def loop(self) -> None:
        # Karton service entrypoint
        raise NotImplementedError

    @StrictClassMethod
    def main(cls) -> None:
        """Main method invoked from CLI."""
        service = cls.karton_from_args()
        service.loop()
