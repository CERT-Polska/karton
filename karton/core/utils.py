import functools
import signal
from contextlib import contextmanager
from typing import Any, Callable, Iterator, Sequence, Tuple, TypeVar

from .exceptions import HardShutdownInterrupt, TaskTimeoutError
from .resource import ResourceBase

T = TypeVar("T")


def chunks(seq: Sequence[T], size: int) -> Iterator[Sequence[T]]:
    return (seq[pos : pos + size] for pos in range(0, len(seq), size))


def walk_resources(obj: Any) -> Iterator[Tuple[str, ResourceBase]]:
    q = [(None, obj)]
    while q:
        last_name, obj = q.pop()
        if isinstance(obj, ResourceBase):
              yield last_name, obj

        elif isinstance(obj, (list, tuple, set)):
              q += [ (last_name, o) for o in obj]

        elif isinstance(obj, dict):
              q += [ (k, v ) for k, v in obj.items() ]


@contextmanager
def timeout(wait_for: int):
    def throw_timeout(signum: int, frame: Any) -> None:
        raise TaskTimeoutError

    original_handler = signal.signal(signal.SIGALRM, throw_timeout)
    try:
        signal.alarm(wait_for)
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, original_handler)


@contextmanager
def graceful_killer(handler: Callable[[], None]):
    """
    Graceful killer for Karton consumers.
    """
    first_try = True

    def signal_handler(signum: int, frame: Any) -> None:
        nonlocal first_try
        handler()
        if signum == signal.SIGINT:
            # Sometimes SIGTERM can be prematurely repeated.
            # Forced but still clean shutdown is implemented only for SIGINT.
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


class StrictClassMethod:
    """
    Like classmethod, but allows calling only when retrieved from class.

    Created to avoid ``KartonClass().main()`` pattern which leads to
    unexpected errors (correct form is ``KartonClass.main()``)
    """

    def __init__(self, func: Callable):
        self.func = func

    def __get__(self, instance: Any, owner: Any):
        @functools.wraps(self.func)
        def newfunc(*args, **kwargs):
            if instance is not None:
                raise TypeError(
                    "This method can be called only on class, not on an instance"
                )
            return self.func(owner, *args, **kwargs)

        return newfunc
