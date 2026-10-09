import asyncio
import contextlib
import logging
import random
import threading
from asyncio import AbstractEventLoop
from typing import (
    Any,
    AsyncGenerator,
    AsyncIterator,
    Coroutine,
    Iterator,
    Protocol,
    Type,
    TypeVar,
)

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed
from websockets.protocol import CLOSED

from karton.core.gateway_protocol import (
    ErrorResponse,
    RequestType,
    Response,
    ResponseType,
)

logger = logging.getLogger(__name__)


class GatewayError(Exception):
    code = ""

    def __init__(self, message: str, code: str | None = None):
        self.code = code or self.code
        self.message = message
        super().__init__(message)


class OperationTimeoutError(GatewayError):
    code = "timeout"


class GatewayBindExpiredError(GatewayError):
    code = "expired_bind"


class GatewayShutdownError(GatewayError):
    code = "shutdown_in_progress"


def make_gateway_error(error_response: ErrorResponse) -> GatewayError:
    for error_class in GatewayError.__subclasses__():
        if error_class.code == error_response.message.code:
            return error_class(error_response.message.error_message)
    return GatewayError(
        message=error_response.message.error_message,
        code=error_response.message.code,
    )


class SessionInitiator(Protocol):

    async def __call__(
        self,
        gateway_client: "AsyncGatewayClient",
        connection: ClientConnection,
        close_on_idle: bool,
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
        session_initiator_callback: SessionInitiator,
        retries: int,
        retry_base_timeout: int,
        retry_jitter: int,
        connect_timeout: int,
        response_timeout: int,
        connection_pool_soft_limit: int,
    ) -> None:
        self.url = url
        self.session_initiator_callback = session_initiator_callback
        self.retries = retries
        self.retry_base_timeout = retry_base_timeout
        self.retry_jitter = retry_jitter
        self.connect_timeout = connect_timeout
        self.response_timeout = response_timeout
        self.connection_pool_soft_limit = connection_pool_soft_limit

        # Main connection that is kept alive
        self._main_connection: ClientConnection | None = None
        self._main_connection_used: bool = False
        # Secondary connections that are closed on idle and free to use
        self._unused_connections: list[ClientConnection] = []
        # Is connector closed?
        self._closed = False

    async def _connect(
        self, retry_state: RetryState, close_on_idle: bool
    ) -> ClientConnection:
        """
        Initiates and returns the connection
        """
        while True:
            connection: ClientConnection | None = None
            try:
                connection = await connect(self.url, open_timeout=self.connect_timeout)
                await self.session_initiator_callback(
                    self, connection, close_on_idle=close_on_idle
                )
                if retry_state.try_no > 0:
                    logger.info("Gateway connection restored.")
                return connection
            except (
                ConnectionError,
                ConnectionClosed,
                TimeoutError,
                GatewayShutdownError,
            ):
                if connection is not None:
                    await connection.close()
                retry_state.next_try()
                if retry_state.last_try():
                    raise
            except BaseException:
                if connection is not None:
                    await connection.close()
                raise
            delay = retry_state.get_retry_delay()
            logger.warning(
                "Failed to connect to the gateway. Retry %d/%d after %.1f seconds",
                retry_state.try_no,
                self.retries,
                delay,
            )
            await asyncio.sleep(delay)

    async def close(self) -> None:
        self._closed = True
        if (
            self._main_connection is not None
            and self._main_connection.state is not CLOSED
        ):
            await self._main_connection.close()
        self._main_connection_used = False
        for connection in self._unused_connections:
            if connection.state is not CLOSED:
                await connection.close()
        self._unused_connections.clear()

    async def _get_available_connection(
        self, retry_state: RetryState
    ) -> ClientConnection:
        """
        Gets available websocket connection.
        """
        if self._closed:
            raise RuntimeError("Gateway client is closed")
        # Drop closed pool connections
        self._unused_connections = [
            connection
            for connection in self._unused_connections
            if connection.state is not CLOSED
        ]
        # Drop closed main connection
        if self._main_connection is not None and self._main_connection.state is CLOSED:
            self._main_connection = None
            self._main_connection_used = False
        # If main connection is disconnected, connect immediately
        if self._main_connection is None:
            self._main_connection = await self._connect(
                retry_state=retry_state, close_on_idle=False
            )
        # If main connection is connected and not used, return it
        if self._main_connection is not None and self._main_connection_used is False:
            self._main_connection_used = True
            return self._main_connection
        if not self._unused_connections:
            return await self._connect(retry_state=retry_state, close_on_idle=True)
        else:
            # LIFO to avoid refreshing old idle connections, letting the
            # server close them when idle.
            return self._unused_connections.pop()

    async def _return_connection(self, connection: ClientConnection) -> None:
        """
        Returns connection to the pool
        """
        if self._closed:
            if connection.state is not CLOSED:
                await connection.close()
            return

        if connection is self._main_connection:
            self._main_connection_used = False
            if connection.state is CLOSED:
                self._main_connection = None
            return

        if connection.state is CLOSED:
            return

        if len(self._unused_connections) >= (self.connection_pool_soft_limit - 1):
            await connection.close()
            return

        self._unused_connections.append(connection)

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
        except (asyncio.CancelledError, GeneratorExit):
            # If connection was gathered for a request
            # that was cancelled in the middle of the operation
            # we should ensure that it is closed before we
            # return it back to the pool
            await connection.close()
            raise
        finally:
            await self._return_connection(connection)

    async def recv[
        ResponseT: ResponseType
    ](
        self,
        connection: ClientConnection,
        expected_response: Type[ResponseT],
    ) -> ResponseT:
        async with asyncio.timeout(self.response_timeout):
            data = await connection.recv()
        message = Response.model_validate_json(data)
        if isinstance(message.root, ErrorResponse):
            raise make_gateway_error(message.root)
        if not isinstance(message.root, expected_response):
            raise RuntimeError(
                f"Got unexpected gateway response: {type(message.root)}, "
                f"expected {expected_response}"
            )
        return message.root

    async def send(self, connection: ClientConnection, request: RequestType) -> None:
        data = request.model_dump_json()
        await connection.send(data)

    async def make_request[
        ResponseT: ResponseType
    ](self, request: RequestType, expected_response: Type[ResponseT],) -> ResponseT:
        retry_state = RetryState(
            max_retries=self.retries,
            base_timeout=self.retry_base_timeout,
            jitter=self.retry_jitter,
        )
        while True:
            request_sent = False
            async with self._connection(retry_state) as connection:
                try:
                    await self.send(connection, request)
                    request_sent = True
                    return await self.recv(connection, expected_response)
                except (
                    ConnectionError,
                    ConnectionClosed,
                    TimeoutError,
                    GatewayShutdownError,
                ) as e:
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

    async def make_streaming_request[
        ResponseT: ResponseType
    ](
        self,
        request: RequestType,
        expected_response: Type[ResponseT],
    ) -> AsyncGenerator[ResponseT, None]:
        retry_state = RetryState(
            max_retries=self.retries,
            base_timeout=self.retry_base_timeout,
            jitter=self.retry_jitter,
        )
        while True:
            async with self._connection(retry_state) as connection:
                try:
                    await self.send(connection, request)
                    while True:
                        yield await self.recv(connection, expected_response)
                except (
                    ConnectionError,
                    ConnectionClosed,
                    TimeoutError,
                    GatewayShutdownError,
                ):
                    retry_state.next_try()
                    if retry_state.last_try():
                        raise
                finally:
                    # Ensure connection is always closed
                    # Streaming requests are non-interruptible
                    # so connection should be discarded after
                    # we exit the receiving loop
                    await connection.close()
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


def _start_event_loop() -> None:
    global _loop
    _loop = asyncio.new_event_loop()
    _loop_ready.set()
    asyncio.set_event_loop(_loop)
    _loop.run_forever()


def _get_threaded_event_loop() -> AbstractEventLoop:
    global _thread
    if _thread is None:
        _thread = threading.Thread(target=_start_event_loop, daemon=True)
        _thread.start()
        _loop_ready.wait()
    if _loop is None:
        raise RuntimeError("Loop was not started")
    return _loop


def run_async(coro: Coroutine[Any, Any, _T]) -> _T:
    loop = _get_threaded_event_loop()
    future = asyncio.run_coroutine_threadsafe(coro, loop)
    return future.result()


def iter_async(async_iterable: AsyncGenerator[_T, None]) -> Iterator[_T]:
    try:
        while True:
            try:
                item = run_async(async_iterable.__anext__())
            except StopAsyncIteration:
                return
            yield item
    finally:
        run_async(async_iterable.aclose())


class SyncGatewayClient:
    def __init__(
        self,
        url: str,
        session_initiator_callback: SessionInitiator,
        retries: int = 5,
        retry_base_timeout=2,
        retry_jitter=1,
        connect_timeout=3,
        response_timeout=5,
        connection_pool_soft_limit=1,
    ) -> None:
        self._async_client = AsyncGatewayClient(
            url=url,
            session_initiator_callback=session_initiator_callback,
            retries=retries,
            retry_base_timeout=retry_base_timeout,
            retry_jitter=retry_jitter,
            connect_timeout=connect_timeout,
            response_timeout=response_timeout,
            connection_pool_soft_limit=connection_pool_soft_limit,
        )

    def close(self) -> None:
        return run_async(self._async_client.close())

    def make_request[
        ResponseT: ResponseType
    ](self, request: RequestType, expected_response: Type[ResponseT],) -> ResponseT:
        return run_async(
            self._async_client.make_request(
                request,
                expected_response,
            )
        )

    def make_streaming_request[
        ResponseT: ResponseType
    ](
        self,
        request: RequestType,
        expected_response: Type[ResponseT],
    ) -> Iterator[
        ResponseT
    ]:
        return iter_async(
            self._async_client.make_streaming_request(
                request,
                expected_response,
            )
        )
