import logging
import platform
import traceback
import warnings
from typing import Any, Callable, Dict

from .backend import KartonBackend
from .task import get_current_task

HOSTNAME = platform.node()


class TaskContextFilter(logging.Filter):
    """
    This is a filter which injects information about current task ID to the log.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        current_task = get_current_task()
        if current_task is not None:
            record.task_id = current_task.task_uid
        else:
            record.task_id = "(no task)"
        return True


class LogLineFormatterMixin:
    format: Callable[[logging.LogRecord], str]

    def prepare_log_line(self, record: logging.LogRecord) -> Dict[str, Any]:
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
        log_line["message"] = self.format(record)

        current_task = get_current_task()
        if current_task is not None:
            log_line["task"] = current_task.serialize()

        log_line["hostname"] = HOSTNAME
        return log_line


class KartonLogHandler(logging.Handler, LogLineFormatterMixin):
    """
    logging.Handler that passes logs to the Karton backend.
    """

    def __init__(self, backend: KartonBackend, channel: str) -> None:
        logging.Handler.__init__(self)
        self.backend = backend
        self.is_consumer_active: bool = True
        self.channel: str = channel

    def emit(self, record: logging.LogRecord) -> None:
        log_line = self.prepare_log_line(record)
        log_consumed = self.backend.produce_log(
            log_line, logger_name=self.channel, level=record.levelname
        )
        if self.is_consumer_active and not log_consumed:
            warnings.warn("There is no active log consumer to receive logged messages.")
        self.is_consumer_active = log_consumed
