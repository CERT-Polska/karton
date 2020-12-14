import abc
import argparse
import logging
import textwrap

from .backend import KartonBackend
from .config import Config
from .logger import KartonLogHandler
from .utils import GracefulKiller


class KartonBase(abc.ABC):
    identity = ""

    def __init__(self, config=None, identity=None):
        self.config = config or Config()
        # If not passed via constructor - get it from class
        if identity is not None:
            self.identity = identity
        self.backend = KartonBackend(self.config)
        self.log_handler = KartonLogHandler(backend=self.backend)
        self.current_task = None

    def setup_logger(self, level=None):
        """
        Setup logger for Karton service (StreamHandler and `karton.logs` handler)

        Called by :py:meth:`Consumer.loop`. If you want to use logger for Producer,
        you need to call it yourself, but remember to set the identity.

        :param level: Logging level. Default is logging.INFO (unless different value is set in Karton config)
        """
        if level is None:
            log_level = logging.INFO
            if self.config.config.has_section("logging"):
                log_level = self.config["logging"].get("level", logging.INFO)
        else:
            log_level = level
        if type(log_level) is str and log_level.isdigit():
            log_level = int(log_level)

        if not self.identity:
            raise ValueError("Can't setup logger without identity")

        logger = logging.getLogger(self.identity)
        logger.setLevel(log_level)
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(
            logging.Formatter("[%(asctime)s][%(levelname)s] %(message)s")
        )
        logger.addHandler(stream_handler)
        logger.addHandler(self.log_handler)

    @property
    def log(self):
        """
        Return Logger instance for Karton service

        If you want to use it in code that is outside of the Consumer class,
        use :func:`logging.getLogger`:

        .. code-block:: python

            import logging
            logging.getLogger("<identity>")
        """
        return logging.getLogger(self.identity)


class KartonServiceBase(KartonBase):
    """
    Karton base class for looping services
    """

    version = None

    def __init__(self, config=None, identity=None):
        super().__init__(config=config, identity=identity)
        self.setup_logger()
        self.shutdown = False
        self.killer = GracefulKiller(self.graceful_shutdown)

    def graceful_shutdown(self):
        self.log.info("Gracefully shutting down!")
        self.shutdown = True

    # Base class for Karton services
    @abc.abstractmethod
    def loop(self):
        # Karton service entrypoint
        raise NotImplementedError

    @classmethod
    def args_description(cls):
        """Return short description for argument parser."""
        if not cls.__doc__:
            return ""
        return textwrap.dedent(cls.__doc__).strip().splitlines()[0]

    @classmethod
    def args_parser(cls):
        """
        Return ArgumentParser for main() class method.

        This method should be overridden if you want to add more arguments.
        """
        parser = argparse.ArgumentParser(description=cls.args_description())
        parser.add_argument("--version", action="version", version=cls.version)
        parser.add_argument("--config-file", help="Alternative configuration path")
        return parser

    @classmethod
    def main(cls):
        """Main method invoked from CLI."""
        parser = cls.args_parser()
        args = parser.parse_args()
        config = Config(args.config_file)
        service = cls(config)
        service.loop()
