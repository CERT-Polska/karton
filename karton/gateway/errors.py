from pydantic import ValidationError


class KartonGatewayError(Exception):
    code: str = "gateway_error"


class BadRequestError(KartonGatewayError):
    code: str = "bad_request"

    def __init__(
        self, message: str, validation_error: ValidationError | None = None
    ) -> None:
        if validation_error is None:
            super().__init__(message)
        else:
            super().__init__(f"{message} ({str(validation_error)})")


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


class InvalidTaskTokenError(KartonGatewayError):
    code: str = "invalid_task_token"


class InvalidTaskStatusError(KartonGatewayError):
    code: str = "invalid_task_status"


class InvalidTaskError(KartonGatewayError):
    code: str = "invalid_task"


class InternalError(KartonGatewayError):
    code: str = "internal_error"
