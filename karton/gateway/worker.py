from gunicorn.workers.gasgi import ASGIWorker

from .shutdown import shutdown_latch


class GatewayASGIWorker(ASGIWorker):
    """
    A gunicorn ASGI worker that cooperates with the gateway shutdown latch.

    Gunicorn's native ASGI worker stops accepting new connections and waits
    up to ``--graceful-timeout`` for in-flight connections to finish before
    cancelling tasks. We additionally flip ``shutdown_latch`` at the start
    of the worker shutdown so that:

    - new requests arriving on already-open idle connections are rejected
      with ``ShutdownError`` (the client gets a clean error and reconnects
      to a healthy instance),
    - the log subscription loop breaks out of its stream early instead of
      streaming until the connection is force-closed.

    Graceful shutdown is triggered by ``SIGTERM`` (gunicorn convention).
    ``SIGINT``/``SIGQUIT`` request a quick shutdown and do not drain.
    """

    async def _shutdown(self):
        await shutdown_latch.request_shutdown()
        await super()._shutdown()
