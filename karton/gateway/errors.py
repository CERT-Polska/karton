class KartonGatewayError(Exception):
    code: str = "gateway_error"


class KartonGatewayTaskError(KartonGatewayError):
    """
    Karton task errors don't disconnect the client in message loop and client
    should handle them without exiting.
    """


class BadRequestError(KartonGatewayError):
    code: str = "bad_request"


class BadCredentialsError(KartonGatewayError):
    code: str = "bad_credentials"


class OperationTimeoutError(KartonGatewayError):
    code: str = "timeout"


class InvalidBindError(KartonGatewayError):
    code: str = "invalid_bind"


class ExpiredBindError(KartonGatewayError):
    code: str = "expired_bind"


class AlreadyBoundError(KartonGatewayError):
    code: str = "already_bound"


class InvalidTaskTokenError(KartonGatewayTaskError):
    code: str = "invalid_task_token"


class InvalidTaskStatusError(KartonGatewayTaskError):
    code: str = "invalid_task_status"


class InvalidTaskError(KartonGatewayTaskError):
    code: str = "invalid_task"


class InternalError(KartonGatewayError):
    code: str = "internal_error"
