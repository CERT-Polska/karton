import abc
import asyncio
from contextlib import contextmanager
from typing import Optional

from karton.core import Task
from karton.core.__version__ import __version__
from karton.core.backend import KartonServiceInfo
from karton.core.base import ConfigMixin, LoggingMixin
from karton.core.config import Config
from karton.core.exceptions import HardShutdownInterrupt
from karton.core.task import get_current_task, set_current_task
from karton.core.utils import StrictClassMethod, graceful_killer

from .backend import KartonAsyncBackend
from .logger import KartonAsyncLogHandler


class KartonAsyncBase(abc.ABC, ConfigMixin, LoggingMixin):
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
        backend: Optional[KartonAsyncBackend] = None,
    ) -> None:
        ConfigMixin.__init__(self, config, identity)

        self.service_info = None
        if self.identity is not None and self.with_service_info:
            self.service_info = KartonServiceInfo(
                identity=self.identity,
                karton_version=__version__,
                service_version=self.version,
            )

        self.backend = backend or KartonAsyncBackend(
            self.config, identity=self.identity, service_info=self.service_info
        )

        log_handler = KartonAsyncLogHandler(backend=self.backend, channel=self.identity)
        LoggingMixin.__init__(self, log_handler)

    @property
    def current_task(self) -> Optional[Task]:
        return get_current_task()

    @current_task.setter
    def current_task(self, task: Optional[Task]):
        set_current_task(task)


class KartonServiceBase(KartonAsyncBase):
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
        backend: Optional[KartonAsyncBackend] = None,
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
    async def loop(self) -> None:
        # Karton service entrypoint
        raise NotImplementedError

    @StrictClassMethod
    def main(cls) -> None:
        """Main method invoked from CLI."""
        service = cls.karton_from_args()
        asyncio.run(service.loop())
