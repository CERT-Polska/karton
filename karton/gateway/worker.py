import logging

from gunicorn.asgi.websocket import WebSocketProtocol
from gunicorn.workers.gasgi import ASGIWorker

from .shutdown import shutdown_latch

logger = logging.getLogger(__name__)


# BUGFIX: remove this after https://github.com/benoitc/gunicorn/pull/3766
# is merged and gunicorn is pinned to fixed version.
# gunicorn's native ASGI worker doesn't deliver websocket.disconnect
# on clean client close (the app gets CancelledError instead).
_original_handle_close = WebSocketProtocol._handle_close


async def _handle_close(self, payload):
    await _original_handle_close(self, payload)
    await self._receive_queue.put(
        {"type": "websocket.disconnect", "code": self.close_code or 1006}
    )


WebSocketProtocol._handle_close = _handle_close  # type: ignore[method-assign]


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
