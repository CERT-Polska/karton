import logging
import traceback
import warnings
from typing import Optional

from .backend import KartonBackend
from .task import Task


class KartonLogHandler(logging.Handler):
    """Base class for karton loggers"""

    def __init__(self, backend: KartonBackend) -> None:
        logging.Handler.__init__(self)
        self.backend = backend
        self.task: Optional[Task] = None
        self.is_consumer_active: bool = True

    def set_task(self, task: Task) -> None:
        self.task = task

    def emit(self, record: logging.LogRecord) -> None:
        ignore_fields = [
            "args",
            "asctime",
            "msecs",
            "msg",
            "pathname",
            "process",
            "processName",
            "relativeCreated",
            "exc_info",
            "exc_text",
            "stack_info",
            "thread",
            "threadName",
        ]
        log_line = {k: v for k, v in record.__dict__.items() if k not in ignore_fields}
        if record.exc_info:
            log_line["excText"] = logging.Formatter().formatException(record.exc_info)
            log_line["excValue"] = str(record.exc_info[1])
            log_line["excTraceback"] = traceback.format_exception(*record.exc_info)

            exc_type = record.exc_info[0]
            if exc_type:
                log_line["excType"] = exc_type.__name__

        log_line["type"] = "log"

        if self.task is not None:
            log_line["task"] = self.task.serialize()

        log_consumed = self.backend.produce_log(
            log_line, logger_name=record.name, level=record.levelname
        )
        if self.is_consumer_active and not log_consumed:
            warnings.warn("There is no active log consumer to receive logged messages.")
        self.is_consumer_active = log_consumed
