import functools
import inspect
import signal
from typing import Any, Callable, Iterator, Sequence, TypeVar

T = TypeVar("T")


def get_function_arg_num(fun: Callable) -> int:
    return len(inspect.signature(fun).parameters)


def chunks(seq: Sequence[T], size: int) -> Iterator[Sequence[T]]:
    return (seq[pos : pos + size] for pos in range(0, len(seq), size))


class GracefulKiller:
    def __init__(self, handle_func: Callable) -> None:
        self.handle_func = handle_func
        self.original_sigint_handler = signal.signal(
            signal.SIGINT, self.exit_gracefully
        )
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, signum: int, frame: Any) -> None:
        self.handle_func()
        if signum == signal.SIGINT:
            signal.signal(signal.SIGINT, self.original_sigint_handler)


class StrictClassMethod:
    """
    Like classmethod, but allows calling only when got from class.

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
