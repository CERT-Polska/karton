import json
import logging
import traceback

LOGS_QUEUE = "karton.logs"


class KartonLogHandler(logging.Handler):
    def __init__(self, rs, **kwargs):
        logging.Handler.__init__(self)
        self.rs = rs
        self.task = None

    def set_task(self, task):
        self.task = task

    def emit(self, record):
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
            log_line["excType"] = record.exc_info[0].__name__
            log_line["excTraceback"] = traceback.format_exception(*record.exc_info)

        log_line["type"] = "log"

        if self.task is not None:
            log_line["task"] = self.task.serialize()

        self.rs.lpush(LOGS_QUEUE, json.dumps(log_line))

    def get_logger(self, identity):
        # Intentionally not using getLogger because we don't want to create singletons!
        logger = logging.Logger(identity or "karton")
        logger.setLevel(logging.DEBUG)
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(
            logging.Formatter("[%(asctime)s][%(levelname)s] %(message)s")
        )
        logger.addHandler(stream_handler)
        logger.addHandler(self)
        return logger
