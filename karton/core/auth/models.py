import json
from dataclasses import dataclass
from typing import Any, Literal, NotRequired, TypedDict

from pydantic import BaseModel, Field

DEFAULT_AUDIENCE = "karton-gateway"
TOKEN_VERSION = 1
# Set of token versions accepted by decode_auth_token.
SUPPORTED_TOKEN_VERSIONS: frozenset[int] = frozenset({TOKEN_VERSION})


class JWKSDict(TypedDict):
    keys: list[dict[str, str]]


class Operation(BaseModel):
    """Base class for operation entitlements"""

    operation: str

    def covers(self, operation: "Operation") -> bool:
        """
        Checks if this entitlement allows the specified operation

        :param operation: Operation to be authorized
        :return: True if operation is authorized by this entitlement, False otherwise
        """
        return isinstance(operation, self.__class__)


class RegisterBindOperation(Operation):
    operation: Literal["register_bind"] = "register_bind"
    filters: list[dict[str, Any]]
    persistent: bool

    def covers(self, operation: "Operation") -> bool:
        """
        Checks if this entitlement allows the specified operation

        :param operation: Operation to be authorized
        :return: True if operation is authorized by this entitlement, False otherwise
        """
        if not isinstance(operation, RegisterBindOperation):
            return False
        if operation.persistent != self.persistent:
            return False
        # Filters are considered equal if their JSON representation is equal
        # regardless of the position in the list.
        # The sorting is shallow, nested lists are order-sensitive
        if sorted(
            json.dumps(filter_bind, sort_keys=True) for filter_bind in self.filters
        ) != sorted(
            json.dumps(filter_bind, sort_keys=True) for filter_bind in operation.filters
        ):
            return False
        return True


class ConsumeTaskOperation(Operation):
    operation: Literal["consume_task"] = "consume_task"


class ProduceTaskOperation(Operation):
    operation: Literal["produce_task"] = "produce_task"
    foreign_buckets: list[str] = Field(default_factory=list)

    def covers(self, operation: "Operation") -> bool:
        """
        Checks if this entitlement allows the specified operation

        :param operation: Operation to be authorized
        :return: True if operation is authorized by this entitlement, False otherwise
        """
        return isinstance(operation, ProduceTaskOperation) and set(
            operation.foreign_buckets
        ).issubset(self.foreign_buckets)


class ConsumeLogOperation(Operation):
    operation: Literal["consume_log"] = "consume_log"


class EnableDebugOperation(Operation):
    operation: Literal["enable_debug"] = "enable_debug"


class InspectKartonOperation(Operation):
    operation: Literal["inspect_karton"] = "inspect_karton"


class RestartTaskOperation(Operation):
    operation: Literal["restart_task"] = "restart_task"


class CancelTaskOperation(Operation):
    operation: Literal["cancel_task"] = "cancel_task"


class RemoveBindOperation(Operation):
    operation: Literal["remove_bind"] = "remove_bind"


class GetMetricsOperation(Operation):
    operation: Literal["get_metrics"] = "get_metrics"


type AllowedOperation = (
    RegisterBindOperation
    | ConsumeTaskOperation
    | ProduceTaskOperation
    | ConsumeLogOperation
    | EnableDebugOperation
    | InspectKartonOperation
    | RestartTaskOperation
    | CancelTaskOperation
    | RemoveBindOperation
    | GetMetricsOperation
)


class AuthClaims(BaseModel):
    """List of operations requested by a Karton service."""

    identity: str
    allowed_operations: list[AllowedOperation]


@dataclass(frozen=True)
class VerifiedGrant:
    """List of operations granted to the Karton service.
    Contains operations requested and subsequently verified by the gateway."""

    claims: AuthClaims
    expires_at: int | None
    token_id: str


class JWTPayload(TypedDict):
    sub: str
    iss: str
    aud: str
    exp: NotRequired[int]
    iat: int
    jti: str
    ver: int
    claims: dict[str, Any]
