import asyncio

from uvicorn.server import Server
from uvicorn.server import logger as uvicorn_logger

from karton.gateway.errors import ShutdownError


class ShutdownLatch:
    """
    Uvicorn terminates all websocket connections when
    shutting down. It's ok, but we would like to finish
    ongoing requests processing and send a response before that.

    In graceful condition, client will be notified about disconnection
    when trying to send the next request, so it can safely reconnect.
    """

    def __init__(self):
        self._pending_requests = 0
        self.shutdown_in_progress = False
        self._pending_requests_fulfilled = asyncio.Event()

    def _start_request(self):
        if self.shutdown_in_progress:
            raise ShutdownError("Request rejected, shutdown is in progress")
        self._pending_requests += 1

    def _stop_request(self):
        if self._pending_requests <= 0:
            raise ValueError("There is no pending request to stop")
        self._pending_requests -= 1
        if self._pending_requests == 0:
            self._pending_requests_fulfilled.set()

    def __enter__(self) -> None:
        self._start_request()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self._stop_request()

    async def request_shutdown(self):
        self.shutdown_in_progress = True
        if self._pending_requests == 0:
            return
        await self._pending_requests_fulfilled.wait()


shutdown_latch = ShutdownLatch()

original_shutdown = Server.shutdown


async def uvicorn_shutdown(self: Server, sockets=None):
    uvicorn_logger.info("Shutdown requested, finalizing ongoing gateway requests")
    # Stop accepting new connections.
    for server in self.servers:
        server.close()
    for sock in sockets or []:
        sock.close()  # pragma: full coverage
    await shutdown_latch.request_shutdown()
    await original_shutdown(self, sockets)


Server.shutdown = uvicorn_shutdown  # type: ignore
