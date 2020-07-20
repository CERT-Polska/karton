import signal


class GracefulKiller:
    def __init__(self, handle_func):
        self.handle_func = handle_func
        self.original_sigint_handler = signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, signum, frame):
        self.handle_func()
        if signum == signal.SIGINT:
            signal.signal(signal.SIGINT, self.original_sigint_handler)
