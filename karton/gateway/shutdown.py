import asyncio

from karton.gateway.errors import ShutdownError


class ShutdownLatch:
    """
    During graceful shutdown we want to let in-flight requests finish
    and respond before the connection is closed, while rejecting new
    requests on already-open idle connections so clients get a clean
    ``ShutdownError`` and can safely reconnect to a healthy instance.

    Gunicorn's native ASGI worker passively waits for connections to
    close (up to ``--graceful-timeout``), so the in-flight draining is
    handled by the worker itself. The latch additionally provides an
    application-level signal (``shutdown_in_progress``) that is checked
    on the request path to reject new work early, and from the log
    subscription loop to break out of a long-running stream.
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
