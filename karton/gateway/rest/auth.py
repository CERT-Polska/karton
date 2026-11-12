from fastapi import Header, HTTPException

from karton.core.auth.models import (
    AllowedOperation,
    CancelTaskOperation,
    GetMetricsOperation,
    InspectKartonOperation,
    RemoveBindOperation,
    RestartTaskOperation,
)

from ..config import gateway_config
from ..session import parse_auth_tokens


class AuthorizationCheck:
    def __init__(self, operation: AllowedOperation) -> None:
        self.operation = operation

    def __call__(self, authorization: str | None = Header(default=None)) -> None:
        if not gateway_config.auth_required:
            return
        if not authorization or not authorization.startswith("Bearer "):
            raise HTTPException(
                status_code=401, detail="Missing or invalid bearer token"
            )
        parts = authorization.split(" ")
        if len(parts) > 2:
            raise HTTPException(
                status_code=401, detail="Missing or invalid bearer token"
            )
        tokens = parts[1].split(",")
        verified_grants = parse_auth_tokens(tokens)
        for grant in verified_grants:
            for op in grant.claims.allowed_operations:
                if op.covers(self.operation):
                    return
        raise HTTPException(
            status_code=403, detail="Client is not authorized to perform this operation"
        )


CanInspectKarton = AuthorizationCheck(InspectKartonOperation())
CanRestartTask = AuthorizationCheck(RestartTaskOperation())
CanCancelTask = AuthorizationCheck(CancelTaskOperation())
CanRemoveBind = AuthorizationCheck(RemoveBindOperation())
CanGetMetrics = AuthorizationCheck(GetMetricsOperation())
