import inspect
import signal
from typing import Any, Callable


def get_function_arg_num(fun: Callable) -> int:
    return len(inspect.signature(fun).parameters)


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
