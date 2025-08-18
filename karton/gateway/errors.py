class KartonGatewayError(Exception):
    def __init__(self, code: str, message: str, details: dict | None = None) -> None:
        self.code = code
        self.message = message
        self.details = details or {}


class KartonGatewayTaskError(KartonGatewayError):
    """
    Karton task errors don't disconnect the client in message loop and client
    should handle them without exiting.
    """


class BadRequestError(KartonGatewayError):
    def __init__(self, details: dict | None = None):
        super().__init__(
            code="bad_request", message="Incorrect request", details=details
        )


class BadCredentialsError(KartonGatewayError):
    def __init__(self):
        super().__init__(code="bad_credentials", message="Incorrect credentials")


class AuthTimeoutError(KartonGatewayError):
    def __init__(self):
        super().__init__(
            code="auth_timeout", message="Client has not authenticated in required time"
        )


class UnsupportedVersionError(KartonGatewayError):
    def __init__(self, client_version: str, required_version: str) -> None:
        super().__init__(
            code="unsupported_version",
            message=f"Client's library version ({client_version}) is unsupported by the gateway (required {required_version})",
        )


class DisallowedIdentityError(KartonGatewayError):
    def __init__(self, details: dict | None = None):
        super().__init__(
            code="disallowed_identity",
            message="Requested identity can't be used by current user",
            details=details,
        )


class DisallowedOutputsError(KartonGatewayTaskError):
    def __init__(self, details: dict | None = None):
        super().__init__(
            code="disallowed_outputs",
            message="Requested outputs can't be used by current user",
            details=details,
        )


class DisallowedTaskError(KartonGatewayTaskError):
    def __init__(self, details: dict | None = None):
        super().__init__(
            code="disallowed_task",
            message="Task headers doesn't match to declared outputs",
            details=details,
        )


class InvalidBindError(KartonGatewayError):
    def __init__(self):
        super().__init__(
            code="invalid_bind",
            message="Incorrect or missing bind for this type of operation",
        )


class ExpiredBindError(KartonGatewayError):
    def __init__(self):
        super().__init__(
            code="expired_bind",
            message="There is a newer version of consumer bind registered",
        )


class AlreadyBoundError(KartonGatewayError):
    def __init__(self):
        super().__init__(
            code="already_bound",
            message="Client is already bound",
        )


class InvalidTaskTokenError(KartonGatewayTaskError):
    def __init__(self, details: dict | None = None):
        super().__init__(
            code="invalid_task_token", message="Incorrect task token", details=details
        )


class NoTaskError(KartonGatewayTaskError):
    def __init__(self):
        super().__init__(
            code="no_task", message="Timeout waiting for a task, try again"
        )


class InternalError(KartonGatewayError):
    def __init__(self):
        super().__init__(code="internal_error", message="Internal server error")
