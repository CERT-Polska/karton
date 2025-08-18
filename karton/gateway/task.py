import dataclasses
import datetime
from typing import Any, Callable, Dict, Iterator

import jwt

from karton.core import Task
from karton.gateway.errors import InvalidTaskTokenError


@dataclasses.dataclass
class TaskTokenInfo:
    task_uid: str
    resources: list[str]


def parse_task_token(token: str, secret_key: str, username: str) -> TaskTokenInfo:
    try:
        token_data = jwt.decode(
            token,
            secret_key,
            algorithms=["HS256"],
            audience=[username],
            options={"require": ["exp", "iss", "sub"]},
        )
        if not token_data["sub"].startswith("karton.task:"):
            raise jwt.exceptions.InvalidSubjectError(
                "Subject of this token is not a karton.task"
            )
        task_uid = token_data["sub"][len("karton.task:") :]
        return TaskTokenInfo(task_uid=task_uid, resources=token_data["resources"])
    except jwt.InvalidTokenError as e:
        raise InvalidTaskTokenError(details={"reason": str(e)})


def make_task_token(
    task_token_info: TaskTokenInfo, secret_key: str, username: str
) -> str:
    issued_at = datetime.datetime.now(datetime.UTC)
    payload = {
        "sub": f"karton.task:{task_token_info.task_uid}",
        "exp": issued_at + datetime.timedelta(days=1),
        "iat": issued_at,
        "iss": "karton.gateway",
        "aud": username,
        "resources": task_token_info.resources,
    }
    return jwt.encode(payload, secret_key, algorithm="HS256")


def _iter_resource_uids(obj: Any) -> Iterator[str]:
    if type(obj) is dict:
        if obj.keys() == {"__karton_resource__"}:
            yield obj["__karton_resource__"]["uid"]
        else:
            for v in obj.values():
                yield from _iter_resource_uids(v)
    elif type(obj) is list:
        for el in obj:
            yield from _iter_resource_uids(el)


def get_task_resources(task: Task) -> list[str]:
    uids = set(resource for resource in _iter_resource_uids(task.payload))
    return list(uids)


def map_resources(obj: Any, mapper: Callable[[dict[str, Any]], dict[str, Any]]) -> Any:
    if type(obj) is dict:
        if obj.keys() == {"__karton_resource__"}:
            return {"__karton_resource__": mapper(obj["__karton_resource__"])}
        else:
            return {k: map_resources(v, mapper) for k, v in obj.items()}
    elif type(obj) is list:
        return [map_resources(el, mapper) for el in obj]
    else:
        return obj
