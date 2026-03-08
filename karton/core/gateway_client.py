import asyncio
import contextlib
import json
import logging
import random
import threading
from typing import Any, AsyncIterator, Coroutine, Iterator, Protocol, TypeVar, cast

from websockets.asyncio.client import ClientConnection, connect
from websockets.protocol import CLOSED

logger = logging.getLogger(__name__)


class GatewayError(Exception):
    code = ""

    def __init__(self, message: str, code: str | None = None):
        self.code = code or self.code
        self.message = message


class OperationTimeoutError(GatewayError):
    code = "timeout"


class GatewayBindExpiredError(GatewayError):
    code = "expired_bind"


class GatewayShutdownError(GatewayError):
    code = "shutdown_in_progress"


def make_gateway_error(code: str, message: str) -> GatewayError:
    for error_class in GatewayError.__subclasses__():
        if error_class.code == code:
            return error_class(message)
    return GatewayError(code, message)


class SessionInitiator(Protocol):
    async def __call__(
        self,
        gateway_client: "AsyncGatewayClient",
        connection: ClientConnection,
        secondary: bool,
    ): ...


class RetryState:
    """
    RetryState keeps current try number and evaluates delay.

    Retry may be triggered at different points of making request and we need
    to share retry state on the request level.
    """

    def __init__(self, max_retries: int, base_timeout: int, jitter: int):
        self.try_no = 0
        self.max_retries = max_retries
        self.base_timeout = base_timeout
        self.jitter = jitter

    def get_retry_delay(self):
        delay = self.base_timeout * (2 ** (self.try_no - 1)) + (
            (random.random() * 2 - 1) * self.jitter
        )
        if delay <= 0:
            delay = self.base_timeout
        return delay

    def next_try(self):
        self.try_no += 1

    def last_try(self) -> bool:
        return (self.try_no - 1) == self.max_retries


class AsyncGatewayClient:
    """
    Websocket client for Karton Gateway

    :param url: URL to connect to.
    :param retries: Number of times to retry the connection.
    :param retry_base_timeout: Base time to retry the connection.
    :param retry_jitter: |
            Random jitter added/subtracted from the delay time
            while retrying the connection.
    :param connect_timeout: Connection timeout in seconds.
    :param response_timeout: Response timeout in seconds.
    :param connection_pool_soft_limit: Connection pool soft limit.
    """

    def __init__(
        self,
        url: str,
        session_initiator: SessionInitiator,
        retries: int,
        retry_base_timeout: int,
        retry_jitter: int,
        connect_timeout: int,
        response_timeout: int,
        connection_pool_soft_limit: int,
    ):
        self.url = url
        self.session_initiator = session_initiator
        self.retries = retries
        self.retry_base_timeout = retry_base_timeout
        self.retry_jitter = retry_jitter
        self.connect_timeout = connect_timeout
        self.response_timeout = response_timeout
        self.connection_pool_soft_limit = connection_pool_soft_limit

        # Main connection used by main consumer loop. It keeps the bind
        # registration and is not terminated by server when it's idle for
        # longer time.
        self._main_connection: ClientConnection | None = None
        self._main_connection_used = False
        # Secondary connections that are free to use
        self._unused_connections: list[ClientConnection] = []

    async def _connect(
        self, secondary: bool, retry_state: RetryState
    ) -> ClientConnection:
        while True:
            connection: ClientConnection | None = None
            try:
                connection = await connect(self.url, open_timeout=self.connect_timeout)
                await self.session_initiator(self, connection, secondary=secondary)
                return connection
            except (ConnectionError, TimeoutError, GatewayShutdownError):
                if connection is not None:
                    await connection.close()
                retry_state.next_try()
                if retry_state.last_try():
                    raise
            delay = retry_state.get_retry_delay()
            logger.warning(
                "Failed to send request to gateway. Retry %d/%d after %.1f seconds",
                retry_state.try_no,
                self.retries,
                delay,
            )
            await asyncio.sleep(delay)

    async def _get_available_connection(
        self, retry_state: RetryState
    ) -> ClientConnection:
        """
        Gets available websocket connection - main or from secondary pool
        """
        if self._main_connection_used:
            # Drop closed connections
            self._unused_connections = [
                connection
                for connection in self._unused_connections
                if connection.state is not CLOSED
            ]
            if not self._unused_connections:
                return await self._connect(secondary=True, retry_state=retry_state)
            else:
                # We intentionally get it in LIFO manner to
                # not constantly refresh old connections. If
                # they are not used, it's unnecessary to keep
                # so many of them, so they got idle and will
                # be closed by server.
                return self._unused_connections.pop()

        self._main_connection_used = True
        if self._main_connection is None or self._main_connection.state is CLOSED:
            try:
                self._main_connection = await self._connect(
                    secondary=False, retry_state=retry_state
                )
            except BaseException:
                self._main_connection_used = False
                raise

        return self._main_connection

    async def _return_connection(self, connection: ClientConnection):
        """
        Returns connection to the pool
        """
        if connection is not self._main_connection:
            if len(self._unused_connections) >= (self.connection_pool_soft_limit - 1):
                await connection.close()
                return
            self._unused_connections.append(connection)
            return

        self._main_connection_used = False
        return

    @contextlib.asynccontextmanager
    async def _connection(
        self, retry_state: RetryState
    ) -> AsyncIterator[ClientConnection]:
        """
        Context manager that gets available connection from the pool
        and returns it back after context exits
        """
        connection = await self._get_available_connection(retry_state)
        try:
            yield connection
        finally:
            await self._return_connection(connection)

    async def recv(
        self,
        connection: ClientConnection,
        expected_response: str = "success",
    ) -> dict[str, Any]:
        async with asyncio.timeout(self.response_timeout):
            data = await connection.recv()
        message = json.loads(data)
        if "response" not in message:
            raise RuntimeError("Incorrect gateway response, missing 'response' key")
        if message["response"] == "error":
            error = message["message"]
            code = error["code"]
            error_message = error["error_message"]
            raise make_gateway_error(code, error_message)
        if message["response"] != expected_response:
            raise RuntimeError(
                f"Got unexpected gateway response: {message['response']}, "
                f"expected {expected_response}"
            )
        return message["message"]

    async def send(
        self, connection: ClientConnection, request: str, message: dict[str, Any]
    ) -> None:
        data = json.dumps(
            {
                "request": request,
                "message": message,
            }
        )
        await connection.send(data)

    async def make_request(
        self, request: str, message: dict[str, Any], expected_response: str
    ) -> dict[str, Any]:
        retry_state = RetryState(
            max_retries=self.retries,
            base_timeout=self.retry_base_timeout,
            jitter=self.retry_jitter,
        )
        while True:
            request_sent = False
            async with self._connection(retry_state) as connection:
                try:
                    await self.send(connection, request, message)
                    request_sent = True
                    return await self.recv(connection, expected_response)
                except (ConnectionError, TimeoutError, GatewayShutdownError) as e:
                    # Ensure connection is closed
                    await connection.close()
                    if not isinstance(e, GatewayShutdownError) and request_sent:
                        # If request was successfully sent and we got
                        # ConnectionError/TimeoutError during waiting
                        # for an answer, we can't safely repeat it
                        # because request could be (partially)
                        # processed by gateway. The best we can do
                        # is to fail operation with an exception
                        raise
                    retry_state.next_try()
                    if retry_state.last_try():
                        raise
            delay = retry_state.get_retry_delay()
            logger.warning(
                "Failed to send request to gateway. Retry %d/%d after %.1f seconds",
                retry_state.try_no,
                self.retries,
                delay,
            )
            await asyncio.sleep(delay)

    async def make_streaming_request(
        self, request: str, message: dict[str, Any], expected_response: str
    ) -> AsyncIterator[dict[str, Any]]:
        retry_state = RetryState(
            max_retries=self.retries,
            base_timeout=self.retry_base_timeout,
            jitter=self.retry_jitter,
        )
        while True:
            async with self._connection(retry_state) as connection:
                try:
                    await self.send(connection, request, message)
                    while True:
                        yield await self.recv(connection, expected_response)
                except (ConnectionError, TimeoutError, GatewayShutdownError):
                    # Ensure connection is closed
                    await connection.close()
                    retry_state.next_try()
                    if retry_state.last_try():
                        raise
            delay = retry_state.get_retry_delay()
            logger.warning(
                "Failed to send request to gateway. Retry %d/%d after %.1f seconds",
                retry_state.try_no,
                self.retries,
                delay,
            )
            await asyncio.sleep(delay)


_loop_ready: threading.Event = threading.Event()
_loop: asyncio.AbstractEventLoop | None = None
_thread: threading.Thread | None = None
_T = TypeVar("_T")


def _start_event_loop():
    global _loop
    _loop = asyncio.new_event_loop()
    _loop_ready.set()
    asyncio.set_event_loop(_loop)
    _loop.run_forever()


def _get_threaded_event_loop():
    global _thread
    if _thread is None:
        _thread = threading.Thread(target=_start_event_loop, daemon=True)
        _thread.start()
        _loop_ready.wait()
    return _loop


def run_async(coro: Coroutine[Any, Any, _T]) -> _T:
    loop = _get_threaded_event_loop()
    future = asyncio.run_coroutine_threadsafe(coro, loop)
    return future.result()


def iter_async(async_iterable: AsyncIterator[_T]) -> Iterator[_T]:
    ait = async_iterable.__aiter__()
    while True:
        try:
            yield run_async(cast(Coroutine[Any, Any, _T], ait.__anext__()))
        except StopAsyncIteration:
            break


class SyncGatewayClient:
    def __init__(
        self,
        url: str,
        session_initiator: SessionInitiator,
        retries: int = 5,
        retry_base_timeout=2,
        retry_jitter=1,
        connect_timeout=3,
        response_timeout=5,
        connection_pool_soft_limit=1,
    ):
        self._async_client = AsyncGatewayClient(
            url=url,
            session_initiator=session_initiator,
            retries=retries,
            retry_base_timeout=retry_base_timeout,
            retry_jitter=retry_jitter,
            connect_timeout=connect_timeout,
            response_timeout=response_timeout,
            connection_pool_soft_limit=connection_pool_soft_limit,
        )

    def make_request(
        self, request: str, message: dict[str, Any], expected_response: str
    ) -> dict[str, Any]:
        return run_async(
            self._async_client.make_request(request, message, expected_response)
        )

    def make_streaming_request(
        self, request: str, message: dict[str, Any], expected_response: str
    ) -> Iterator[dict[str, Any]]:
        return iter_async(
            self._async_client.make_streaming_request(
                request, message, expected_response
            )
        )
