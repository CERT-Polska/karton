import json
from dataclasses import dataclass
from typing import Any, Literal, TypedDict, Union

from pydantic import BaseModel, Field, RootModel

DEFAULT_AUDIENCE = "karton-gateway"
TOKEN_VERSION = 1
# Set of token versions accepted by decode_auth_token. Keep the current
# TOKEN_VERSION here; retain older versions during a grace period when
# bumping TOKEN_VERSION with breaking ACL-semantic changes.
SUPPORTED_TOKEN_VERSIONS: frozenset[int] = frozenset({TOKEN_VERSION})


class JWKSDict(TypedDict):
    keys: list[dict[str, str]]


class AllowedRegisterBind(BaseModel):
    operation: Literal["register_bind"] = "register_bind"
    filters: list[dict[str, Any]]
    persistent: bool

    def covers(self, required: "AllowedOperation") -> bool:
        if not isinstance(required, AllowedRegisterBind):
            return False
        if required.persistent != self.persistent:
            return False
        # Filters are considered equal if their JSON representation is equal
        # regardless of the position in the list.
        if sorted(
            json.dumps(filter_bind, sort_keys=True) for filter_bind in self.filters
        ) != sorted(
            json.dumps(filter_bind, sort_keys=True) for filter_bind in required.filters
        ):
            return False
        return True


class AllowedConsumeTask(BaseModel):
    operation: Literal["consume_task"] = "consume_task"

    def covers(self, required: "AllowedOperation") -> bool:
        return isinstance(required, AllowedConsumeTask)


class AllowedProduceTask(BaseModel):
    operation: Literal["produce_task"] = "produce_task"
    foreign_buckets: list[str] = Field(default_factory=list)

    def covers(self, required: "AllowedOperation") -> bool:
        return isinstance(required, AllowedProduceTask) and set(
            required.foreign_buckets
        ).issubset(self.foreign_buckets)


class AllowedConsumeLog(BaseModel):
    operation: Literal["consume_log"] = "consume_log"

    def covers(self, required: "AllowedOperation") -> bool:
        return isinstance(required, AllowedConsumeLog)


AllowedOperation = Union[
    AllowedRegisterBind,
    AllowedConsumeTask,
    AllowedProduceTask,
    AllowedConsumeLog,
]


class Request(RootModel):
    root: AllowedOperation = Field(discriminator="operation")


class AuthClaims(BaseModel):
    identity: str
    allowed_operations: list[AllowedOperation]


@dataclass(frozen=True)
class VerifiedGrant:
    claims: AuthClaims
    expires_at: int | None
    token_id: str
