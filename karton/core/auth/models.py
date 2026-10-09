import json
from dataclasses import dataclass
from typing import Any, Literal, NotRequired, TypedDict

from pydantic import BaseModel, Field

DEFAULT_AUDIENCE = "karton-gateway"
TOKEN_VERSION = 1
# Set of token versions accepted by decode_auth_token. Keep the current
# TOKEN_VERSION here; retain older versions during a grace period when
# bumping TOKEN_VERSION with breaking ACL-semantic changes.
SUPPORTED_TOKEN_VERSIONS: frozenset[int] = frozenset({TOKEN_VERSION})


class JWKSDict(TypedDict):
    keys: list[dict[str, str]]


class RegisterBindOperation(BaseModel):
    operation: Literal["register_bind"] = "register_bind"
    filters: list[dict[str, Any]]
    persistent: bool

    def covers(self, operation: "AllowedOperation") -> bool:
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


class ConsumeTaskOperation(BaseModel):
    operation: Literal["consume_task"] = "consume_task"

    def covers(self, operation: "AllowedOperation") -> bool:
        """
        Checks if this entitlement allows the specified operation

        :param operation: Operation to be authorized
        :return: True if operation is authorized by this entitlement, False otherwise
        """
        return isinstance(operation, ConsumeTaskOperation)


class ProduceTaskOperation(BaseModel):
    operation: Literal["produce_task"] = "produce_task"
    foreign_buckets: list[str] = Field(default_factory=list)

    def covers(self, operation: "AllowedOperation") -> bool:
        """
        Checks if this entitlement allows the specified operation

        :param operation: Operation to be authorized
        :return: True if operation is authorized by this entitlement, False otherwise
        """
        return isinstance(operation, ProduceTaskOperation) and set(
            operation.foreign_buckets
        ).issubset(self.foreign_buckets)


class ConsumeLogOperation(BaseModel):
    operation: Literal["consume_log"] = "consume_log"

    def covers(self, operation: "AllowedOperation") -> bool:
        """
        Checks if this entitlement allows the specified operation

        :param operation: Operation to be authorized
        :return: True if operation is authorized by this entitlement, False otherwise
        """
        return isinstance(operation, ConsumeLogOperation)


type AllowedOperation = (
    RegisterBindOperation
    | ConsumeTaskOperation
    | ProduceTaskOperation
    | ConsumeLogOperation
)


class AuthClaims(BaseModel):
    identity: str
    allowed_operations: list[AllowedOperation]


@dataclass(frozen=True)
class VerifiedGrant:
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
