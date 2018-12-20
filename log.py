import json
import logging
import traceback

import pika


class RabbitMQHandler(logging.Handler):
    def __init__(self):
        logging.Handler.__init__(self)
        connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
        self.channel = connection.channel()

    def emit(self, record):
        ignore_fields = ["args", "asctime", "msecs", "msg", "pathname", "process", "processName", "relativeCreated",
                         "exc_info", "exc_text", "stack_info", "thread", "threadName"]
        log_line = {k: v for k, v in record.__dict__.items() if k not in ignore_fields}
        if record.exc_info:
            log_line["excText"] = logging.Formatter().formatException(record.exc_info)
            log_line["excValue"] = str(record.exc_info[1])
            log_line["excType"] = record.exc_info[0].__name__
            log_line["excTraceback"] = traceback.format_exception(*record.exc_info)

        self.channel.basic_publish("karton.logs", "", json.dumps(log_line), pika.BasicProperties())

    def close(self):
        self.channel.close()


def setup_logging():
    logging.basicConfig(level=logging.INFO)
    handler = RabbitMQHandler()
    logging.getLogger().addHandler(handler)
