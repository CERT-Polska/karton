import abc
import asyncio
import signal
from asyncio import CancelledError
from typing import Optional

from karton.core import Task
from karton.core.__version__ import __version__
from karton.core.backend import KartonServiceInfo
from karton.core.base import ConfigMixin, LoggingMixin
from karton.core.config import Config
from karton.core.task import get_current_task, set_current_task
from karton.core.utils import StrictClassMethod

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
        LoggingMixin.__init__(
            self,
            log_handler,
            log_format="[%(asctime)s][%(levelname)s][%(task_id)s] %(message)s",
        )

    async def connect(self) -> None:
        await self.backend.connect()

    @property
    def current_task(self) -> Optional[Task]:
        return get_current_task()

    @current_task.setter
    def current_task(self, task: Optional[Task]):
        set_current_task(task)


class KartonAsyncServiceBase(KartonAsyncBase):
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
        self._loop_coro: Optional[asyncio.Task] = None

    def _do_shutdown(self) -> None:
        self.log.info("Got signal, shutting down...")
        if self._loop_coro is not None:
            self._loop_coro.cancel()

    @abc.abstractmethod
    async def _loop(self) -> None:
        raise NotImplementedError

    # Base class for Karton services
    async def loop(self) -> None:
        if self.enable_publish_log and hasattr(self.log_handler, "start_consuming"):
            self.log_handler.start_consuming()
        await self.connect()
        event_loop = asyncio.get_event_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            event_loop.add_signal_handler(sig, self._do_shutdown)
        self._loop_coro = asyncio.create_task(self._loop())
        try:
            await self._loop_coro
        finally:
            for sig in (signal.SIGTERM, signal.SIGINT):
                event_loop.remove_signal_handler(sig)
            if self.enable_publish_log and hasattr(self.log_handler, "stop_consuming"):
                await self.log_handler.stop_consuming()

    @StrictClassMethod
    def main(cls) -> None:
        """Main method invoked from CLI."""
        service = cls.karton_from_args()
        try:
            asyncio.run(service.loop())
        except CancelledError:
            # Swallow cancellation, we're done!
            pass
