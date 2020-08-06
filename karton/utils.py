import signal
import inspect
import sys


def get_user_input():
    if sys.version_info[0] == 2:
        return raw_input()  # noqa
    else:
        return input()


def get_function_arg_num(fun):
    if sys.version_info[0] == 2:
        return len(inspect.getargspec(fun).args)
    else:
        return len(inspect.signature(fun).parameters)


class GracefulKiller:
    def __init__(self, handle_func):
        self.handle_func = handle_func
        self.original_sigint_handler = signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, signum, frame):
        self.handle_func()
        if signum == signal.SIGINT:
            signal.signal(signal.SIGINT, self.original_sigint_handler)
