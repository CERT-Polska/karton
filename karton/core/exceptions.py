class InvalidIdentityError(Exception):
    pass


class TaskTimeoutError(Exception):
    pass


class HardShutdownInterrupt(BaseException):
    pass
