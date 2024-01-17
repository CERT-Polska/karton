class InvalidIdentityError(Exception):
    pass


class TaskTimeoutError(BaseException):
    pass


class HardShutdownInterrupt(BaseException):
    pass
