import signal


class GracefulKiller:
    def __init__(self, handle_func):
        self.handle_func = handle_func
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, signum, frame):
        self.handle_func()
