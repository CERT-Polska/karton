import enum
import json
from typing import Any

from pydantic import BaseModel, Field


class CapabilityClaim(enum.Enum):
    produce_task = "produce_task"
    consume_task = "consume_task"
    consume_log = "consume_log"


class BindClaim(BaseModel):
    filters: list[dict[str, Any]]
    persistent: bool

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, BindClaim):
            return False
        if self.persistent != other.persistent:
            return False
        if sorted([json.dumps(filter_bind) for filter_bind in self.filters]) != sorted(
            [json.dumps(filter_bind) for filter_bind in other.filters]
        ):
            return False
        return True


class AuthClaims(BaseModel):
    identity: str
    binds: list[BindClaim] | None = None
    capabilities: list[CapabilityClaim] = Field(default_factory=list)
    foreign_buckets: list[str] = Field(default_factory=list)
