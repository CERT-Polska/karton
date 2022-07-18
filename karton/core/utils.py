import inspect
import signal
from contextlib import contextmanager
from typing import Any, Callable, Iterator, Sequence, TypeVar

T = TypeVar("T")


def get_function_arg_num(fun: Callable) -> int:
    return len(inspect.signature(fun).parameters)


def chunks(seq: Sequence[T], size: int) -> Iterator[Sequence[T]]:
    return (seq[pos : pos + size] for pos in range(0, len(seq), size))


class HardShutdownInterrupt(BaseException):
    pass


@contextmanager
def graceful_killer(handler: Callable[[], None]):
    """
    Graceful killer for Karton consumers.
    """
    first_try = True

    def signal_handler(signum: int, frame: Any) -> None:
        nonlocal first_try
        handler()
        if not first_try:
            raise HardShutdownInterrupt()
        first_try = False

    original_sigint_handler = signal.signal(signal.SIGINT, signal_handler)
    original_sigterm_handler = signal.signal(signal.SIGTERM, signal_handler)
    try:
        yield
    finally:
        signal.signal(signal.SIGINT, original_sigint_handler)
        signal.signal(signal.SIGTERM, original_sigterm_handler)
