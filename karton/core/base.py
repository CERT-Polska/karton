import abc
import argparse
import logging
import textwrap
from typing import Optional, Union, cast

from .backend import KartonBackend
from .config import Config
from .logger import KartonLogHandler
from .task import Task
from .utils import GracefulKiller


class KartonBase(abc.ABC):
    identity = ""

    def __init__(
        self,
        config: Optional[Config] = None,
        identity: Optional[str] = None,
        backend: Optional[KartonBackend] = None,
    ) -> None:
        # If not passed via constructor - get it from class
        if identity is not None:
            self.identity = identity

        self.config = config or Config()
        self.backend = backend or KartonBackend(self.config)

        self.log_handler = KartonLogHandler(backend=self.backend, channel=self.identity)
        self.current_task: Optional[Task] = None

    def setup_logger(self, level: Optional[Union[str, int]] = None) -> None:
        """
        Setup logger for Karton service (StreamHandler and `karton.logs` handler)

        Called by :py:meth:`Consumer.loop`. If you want to use logger for Producer,
        you need to call it yourself, but remember to set the identity.

        :param level: Logging level. Default is logging.INFO \
                      (unless different value is set in Karton config)
        """
        if level is None:
            if self.config.config.has_section("logging"):
                level = self.config["logging"].get("level", logging.INFO)
            else:
                level = logging.INFO

        if type(level) is str and cast(str, level).isdigit():
            log_level: Union[str, int] = int(level)
        else:
            log_level = level

        if not self.identity:
            raise ValueError("Can't setup logger without identity")

        self.log_handler.setFormatter(logging.Formatter())

        logger = logging.getLogger(self.identity)
        logger.setLevel(log_level)
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(
            logging.Formatter("[%(asctime)s][%(levelname)s] %(message)s")
        )
        logger.addHandler(stream_handler)
        logger.addHandler(self.log_handler)

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


class KartonServiceBase(KartonBase):
    """
    Karton base class for looping services.

    You can set a informative version information by setting the ``version`` class
    attribute

    :param config: Karton config to use for service configuration
    :param identity: Karton service identity to use
    :param backend: Karton backend to use
    """

    version: Optional[str] = None

    def __init__(
        self,
        config: Optional[Config] = None,
        identity: Optional[str] = None,
        backend: Optional[KartonBackend] = None,
    ) -> None:
        super().__init__(config=config, identity=identity, backend=backend)
        self.setup_logger()
        self.shutdown = False
        self.killer = GracefulKiller(self.graceful_shutdown)

    def graceful_shutdown(self) -> None:
        self.log.info("Gracefully shutting down!")
        self.shutdown = True

    # Base class for Karton services
    @abc.abstractmethod
    def loop(self) -> None:
        # Karton service entrypoint
        raise NotImplementedError

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

        This method should be overridden if you want to add more arguments.
        """
        parser = argparse.ArgumentParser(description=cls.args_description())
        parser.add_argument(
            "--version", action="version", version=cast(str, cls.version)
        )
        parser.add_argument("--config-file", help="Alternative configuration path")
        return parser

    @classmethod
    def main(cls) -> None:
        """Main method invoked from CLI."""
        parser = cls.args_parser()
        args = parser.parse_args()
        config = Config(args.config_file)
        service = cls(config)
        service.loop()
