import time
from functools import wraps

import pika
import logging

logger = logging.getLogger("rmq")

class RabbitMQConnection(object):
    RECONNECT_ATTEMPTS = 20
    RECONNECT_DELAY = 5

    def __init__(self, parameters):
        self._parameters = parameters
        self._connection = self.__connect()

    def __connect(self):
        for attempts in range(self.RECONNECT_ATTEMPTS):
            try:
                logger.info("Connecting to broker...")
                self._connection = pika.BlockingConnection(self._parameters)
                return self._connection
            except pika.exceptions.AMQPConnectionError as e:
                logger.exception("[{}/{}] Reconnecting after {} seconds - {}".format(
                    attempts + 1,
                    self.RECONNECT_ATTEMPTS,
                    self.RECONNECT_DELAY,
                    repr(e)
                ))
                time.sleep(self.RECONNECT_DELAY)
        raise RuntimeError("AMQP broker is unavailable")

    def channel(self):
        return self._connection.channel()


class RabbitMQChannel(object):
    def __init__(self, rmq_connection):
        self._rmq_connection = rmq_connection
        self._channel = None

    def __getattr__(self, item):
        if self._channel is None or not self._channel.is_open:
            self._channel = self._rmq_connection.channel()
            logger.debug("Created new channel")
        return getattr(self._channel, item)


class RabbitMQClient(object):
    def __init__(self, connection=None, parameters=None):
        if connection is not None:
            self.connection = connection
        else:
            self.connection = RabbitMQConnection(parameters)
        self.channel = RabbitMQChannel(self.connection)

    @staticmethod
    def retryable(f):
        @wraps(f)
        def retryable_method(self, *args, **kwargs):
            while True:
                try:
                    return f(self, *args, **kwargs)
                except (pika.exceptions.AMQPConnectionError, pika.exceptions.AMQPChannelError) as e:
                    self.connection.__connect()
                logger.debug("Retrying {} after connection break...".format(f.__name__))
        return retryable_method
