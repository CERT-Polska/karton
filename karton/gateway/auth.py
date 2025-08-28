import dataclasses
import secrets

from argon2 import PasswordHasher
from argon2.exceptions import VerifyMismatchError
from redis.asyncio import Redis

hasher = PasswordHasher()
DUMMY_HASH = hasher.hash(secrets.token_bytes(16))


@dataclasses.dataclass
class User:
    username: str


def get_anonymous_user() -> User:
    return User(username="<anonymous>")


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

    return User(username=username)
