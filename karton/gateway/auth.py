import dataclasses
import secrets
from typing import Any

from argon2 import PasswordHasher
from argon2.exceptions import VerifyMismatchError
from redis.asyncio import Redis

hasher = PasswordHasher()
DUMMY_HASH = hasher.hash(secrets.token_bytes(16))


@dataclasses.dataclass
class User:
    username: str
    allowed_identities: list[str]
    allowed_outputs: list[dict[str, Any]]


async def authorize_user(rs: Redis, username: str, password: str) -> User | None:
    user_record = await rs.hgetall(f"karton.user:{username}")

    if not user_record:
        # Some dummy hashing to avoid user enumeration
        try:
            hasher.verify(DUMMY_HASH, password)
        except VerifyMismatchError:
            pass
        return None

    try:
        hasher.verify(user_record["password"], password)
    except VerifyMismatchError:
        return None

    return User(
        username=username,
        allowed_identities=user_record["allowed_identities"],
        allowed_outputs=user_record["allowed_outputs"],
    )


def validate_identity(identity: str, allowed_identities: list[str]) -> bool:
    return True


def validate_outputs(outputs: dict[str, Any], allowed_outputs: list[str]) -> bool:
    return True
