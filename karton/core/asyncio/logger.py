"""
asyncio implementation of KartonLogHandler

Logging methods must be non-blocking, so Redis forwarder must be
scheduled on separate thread. We assume here that KartonAsyncBackend.produce_log
is thread-safe when Redis client is shared by two asyncio loops.
"""

import asyncio
import logging
import platform
import threading
from queue import SimpleQueue
from typing import Any, Dict, Optional, Tuple

from karton.core.logger import LogLineFormatterMixin

from .backend import KartonAsyncBackend

HOSTNAME = platform.node()

QueuedRecord = Optional[Tuple[Dict[str, Any], str]]


def async_log_consumer(
    queue: SimpleQueue[QueuedRecord], backend: KartonAsyncBackend, channel: str
) -> None:
    loop = asyncio.new_event_loop()
    try:
        while True:
            item = queue.get()
            if not item:
                break
            log_line, levelname = item
            loop.run_until_complete(
                backend.produce_log(log_line, logger_name=channel, level=levelname)
            )
    finally:
        loop.close()


class KartonAsyncLogHandler(logging.Handler, LogLineFormatterMixin):
    """
    logging.Handler that passes logs to the Karton backend.
    """

    def __init__(self, backend: KartonAsyncBackend, channel: str) -> None:
        logging.Handler.__init__(self)
        self._queue: SimpleQueue[QueuedRecord] = SimpleQueue()
        self._consumer = threading.Thread(
            target=async_log_consumer,
            args=(
                self._queue,
                backend,
                channel,
            ),
        )

    def emit(self, record: logging.LogRecord) -> None:
        log_line = self.prepare_log_line(record)
        self._queue.put_nowait((log_line, record.levelname))

    def start_consuming(self):
        self._consumer.start()

    def stop_consuming(self):
        self._queue.put_nowait(None)  # Signal that queue is finished
        self._consumer.join()
