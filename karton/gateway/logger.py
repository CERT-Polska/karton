import contextvars
import logging
import secrets

__connection_id: contextvars.ContextVar[str] = contextvars.ContextVar("connection_id")


def set_connection_id():
    __connection_id.set(secrets.token_hex(16))


def get_connection_id() -> str | None:
    return __connection_id.get()


class ConnectionLoggingFilter(logging.Filter):
    """
    This is a filter which injects information about connection ID
    for easier log correclation
    """

    def filter(self, record: logging.LogRecord) -> bool:
        conn_id = get_connection_id()
        if conn_id is not None:
            record.connection_id = get_connection_id()
        else:
            record.connection_id = None
        return True


def setup_logger():
    formatter = logging.Formatter(
        "[%(asctime)s][%(levelname)s][%(connection_id)s] %(message)s"
    )

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    stream_handler.addFilter(ConnectionLoggingFilter())

    gateway_logger = logging.getLogger("karton.gateway")
    gateway_logger.addHandler(stream_handler)
    gateway_logger.setLevel(logging.INFO)
    gateway_logger.propagate = False

    logging.basicConfig(level=logging.INFO)
