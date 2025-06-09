"""
asyncio implementation of KartonLogHandler
"""

import asyncio
import logging
import platform
from typing import Any, Dict, Optional, Tuple

from karton.core.logger import LogLineFormatterMixin

from .backend import KartonAsyncBackend

HOSTNAME = platform.node()

QueuedRecord = Optional[Tuple[Dict[str, Any], str]]


async def async_log_consumer(
    queue: asyncio.Queue[QueuedRecord], backend: KartonAsyncBackend, channel: str
) -> None:
    while True:
        item = await queue.get()
        if not item:
            break
        log_line, levelname = item
        await backend.produce_log(log_line, logger_name=channel, level=levelname)


class KartonAsyncLogHandler(logging.Handler, LogLineFormatterMixin):
    """
    logging.Handler that passes logs to the Karton backend.
    """

    def __init__(self, backend: KartonAsyncBackend, channel: str) -> None:
        logging.Handler.__init__(self)
        self._consumer: Optional[asyncio.Task] = None
        self._queue: asyncio.Queue[QueuedRecord] = asyncio.Queue()
        self._backend = backend
        self._channel = channel

    def emit(self, record: logging.LogRecord) -> None:
        log_line = self.prepare_log_line(record)
        self._queue.put_nowait((log_line, record.levelname))

    def start_consuming(self):
        if self._consumer is not None:
            raise RuntimeError("Consumer already started")
        self._consumer = asyncio.create_task(
            async_log_consumer(self._queue, self._backend, self._channel)
        )

    async def stop_consuming(self):
        if self._consumer is None:
            raise RuntimeError("Consumer is not started")
        self._queue.put_nowait(None)  # Signal that queue is finished
        await self._consumer
