import logging
import platform

from .backend import KartonAsyncBackend

HOSTNAME = platform.node()


class KartonAsyncLogHandler(logging.Handler):
    """
    logging.Handler that passes logs to the Karton backend.
    """

    def __init__(self, backend: KartonAsyncBackend, channel: str) -> None:
        logging.Handler.__init__(self)
        self.backend = backend
        self.is_consumer_active: bool = True
        self.channel: str = channel

    def emit(self, record: logging.LogRecord) -> None: ...
