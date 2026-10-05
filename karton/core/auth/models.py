import json
from typing import Any, Literal, Union

from pydantic import BaseModel, Field, RootModel

DEFAULT_AUDIENCE = "karton-gateway"


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
