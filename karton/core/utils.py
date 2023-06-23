import functools
import itertools
import signal
from contextlib import contextmanager
from typing import Any, Callable, Iterator, Sequence, Tuple, TypeVar

from .exceptions import HardShutdownInterrupt, TaskTimeoutError

T = TypeVar("T")


def chunks(seq: Sequence[T], size: int) -> Iterator[Sequence[T]]:
    return (seq[pos : pos + size] for pos in range(0, len(seq), size))


def chunks_iter(seq: Iterator[T], size: int) -> Iterator[Sequence[T]]:
    # We need to ensure that seq is iterator, so this method works correctly
    it = iter(seq)
    while True:
        elements = list(itertools.islice(it, size))
        if len(elements) == 0:
            return
        yield elements


def recursive_iter(obj: Any) -> Iterator[Any]:
    """
    Yields all values recursively from nested list/dict structures

    :param obj: Object to iterate over
    """
    if isinstance(obj, (list, tuple)):
        for elem in obj:
            yield from recursive_iter(elem)
    elif isinstance(obj, dict):
        for elem in obj.values():
            yield from recursive_iter(elem)
    else:
        yield obj


def recursive_iter_with_keys(obj: Any, name: str = "") -> Iterator[Tuple[str, Any]]:
    """
    Yields (path, value) tuples recursively from nested list/dict structures

    :param obj: Object to iterate over
    :param name: Object name
    """
    if isinstance(obj, (list, tuple)):
        for idx, elem in enumerate(obj):
            yield from recursive_iter_with_keys(elem, name=f"{name}.{idx}")
    elif isinstance(obj, dict):
        for key, elem in obj.items():
            yield from recursive_iter_with_keys(elem, name=f"{name}.{key}")
    else:
        yield name, obj


def recursive_map(func: Callable[[Any], Any], obj: Any) -> Any:
    """
    Returns copy of collection with recursively mapped elements

    :param func: Mapping function
    :param obj: Object to iterate over
    """
    mapped_obj = func(obj)
    if isinstance(mapped_obj, (list, tuple)):
        return [recursive_map(func, elem) for elem in mapped_obj]
    elif isinstance(mapped_obj, dict):
        return {key: recursive_map(func, elem) for key, elem in mapped_obj.items()}
    else:
        return mapped_obj


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
