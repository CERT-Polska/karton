import dataclasses
import datetime
from typing import Any, Callable, Iterator

import jwt
from pydantic import ValidationError

from karton.core.resource import ResourceBase
from karton.core.task import Task, TaskState
from karton.gateway.backend import gateway_backend
from karton.gateway.errors import InvalidTaskError, InvalidTaskTokenError
from karton.gateway.models import DeclaredResourceSpec, ResourceUrl


@dataclasses.dataclass
class TaskTokenInfo:
    task_uid: str
    resources: list[str]


PayloadBags = tuple[dict[str, Any], dict[str, Any]]


def parse_task_token(token: str, secret_key: str, audience: str) -> TaskTokenInfo:
    try:
        token_data = jwt.decode(
            token,
            secret_key,
            algorithms=["HS256"],
            audience=[audience],
            options={"require": ["exp", "iss", "sub"]},
        )
        if not token_data["sub"].startswith("karton.task:"):
            raise jwt.exceptions.InvalidSubjectError(
                "Subject of this token is not a karton.task"
            )
        task_uid = token_data["sub"][len("karton.task:") :]
        return TaskTokenInfo(task_uid=task_uid, resources=token_data["resources"])
    except jwt.InvalidTokenError as e:
        raise InvalidTaskTokenError(f"Invalid task token: {type(e)} - {str(e)}")


def make_task_token(
    task_token_info: TaskTokenInfo, secret_key: str, audience: str
) -> str:
    issued_at = datetime.datetime.now(datetime.timezone.utc)
    payload = {
        "sub": f"karton.task:{task_token_info.task_uid}",
        "exp": issued_at + datetime.timedelta(days=1),
        "iat": issued_at,
        "iss": "karton.gateway",
        "aud": audience,
        "resources": task_token_info.resources,
    }
    return jwt.encode(payload, secret_key, algorithm="HS256")


def iter_resources(obj: Any) -> Iterator[dict[str, Any]]:
    if type(obj) is dict:
        if obj.keys() == {"__karton_resource__"}:
            yield obj["__karton_resource__"]
        else:
            for v in obj.values():
                yield from iter_resources(v)
    elif type(obj) in (list, tuple):
        for el in obj:
            yield from iter_resources(el)


def map_resources(obj: Any, mapper: Callable[[dict[str, Any]], Any]) -> Any:
    if type(obj) is dict:
        if obj.keys() == {"__karton_resource__"}:
            return mapper(obj["__karton_resource__"])
        else:
            return {k: map_resources(v, mapper) for k, v in obj.items()}
    elif type(obj) in (list, tuple):
        return [map_resources(el, mapper) for el in obj]
    else:
        return obj


def process_declared_task_resources(
    payload_bags: PayloadBags,
    allowed_parent_resources: list[str],
    allowed_buckets: list[str],
) -> tuple[PayloadBags, list[DeclaredResourceSpec]]:
    """
    Performs a lookup for resources referenced in payload bags, validates them
    and maps them to ResourceBase objects for Task.serialize

    :param payload_bags: Payload bags to process
    :param allowed_parent_resources: Allowed remote resources referenced in parent token
    :param allowed_buckets: Allowed target buckets to use for resource upload
    :return: |
        Tuple with two elements: payload bags with mapped resources to ResourceBase and
        list of DeclaredResourceSpec objects with validated resource specification.
    """
    resources: dict[str, DeclaredResourceSpec] = {}

    def process_resource(resource_data: dict[str, Any]) -> Any:
        try:
            resource_spec = DeclaredResourceSpec.model_validate(resource_data)
        except ValidationError as e:
            raise InvalidTaskError(f"Invalid resource specification {e}")
        if (
            not resource_spec.to_upload
            and resource_spec.uid not in allowed_parent_resources
        ):
            raise InvalidTaskError(
                f"Service is not allowed to reference resource '{resource_spec.uid}'"
            )
        if resource_spec.bucket is None:
            resource_spec.bucket = gateway_backend.default_bucket_name
        elif resource_spec.bucket not in allowed_buckets:
            raise InvalidTaskError(
                f"Service is not allowed to reference bucket '{resource_spec.bucket}' "
                f"in resource '{resource_spec.uid}'"
            )
        if resource_spec.uid not in resources:
            resources[resource_spec.uid] = resource_spec
        return ResourceBase(
            _uid=resource_spec.uid,
            _size=resource_spec.size,
            name=resource_spec.name,
            metadata=resource_spec.metadata,
            sha256=resource_spec.sha256,
            bucket=resource_spec.bucket,
        )

    transformed_payload_bags = map_resources(payload_bags, process_resource)
    return transformed_payload_bags, list(resources.values())


async def generate_resource_upload_urls(
    resources: list[DeclaredResourceSpec],
) -> list[ResourceUrl]:
    """
    Generates a list of presigned upload URLs for resources marked as "to_upload"

    :param resources: List of resource specifications
    :return: List of ResourceUrl objects with presigned upload URLs
    """
    upload_urls: list[ResourceUrl] = []
    for resource in resources:
        if resource.to_upload:
            bucket = resource.bucket
            if bucket is None:
                raise RuntimeError("Bucket can't be None")
            upload_url = await gateway_backend.get_presigned_object_upload_url(
                bucket=bucket, object_uid=resource.uid
            )
            upload_urls.append(
                ResourceUrl(
                    uid=resource.uid,
                    url=upload_url,
                )
            )
    return upload_urls


async def generate_resource_download_urls(
    task: Task,
    allowed_buckets: list[str],
) -> list[ResourceUrl]:
    """
    Generates a list of presigned download URLs for an incoming task

    :param task: Task containing resources
    :param allowed_buckets: Allowed target buckets to use for resource download
    :return: List of ResourceUrl objects with presigned download URLs
    """
    resources = {}

    for resource in task.iterate_resources():
        if resource.uid in resources:
            continue
        if resource.bucket not in allowed_buckets:
            raise InvalidTaskError(
                f"Got task that references bucket '{resource.bucket}' "
                f"that can't be handled by Karton Gateway"
            )
        download_url = await gateway_backend.get_presigned_object_download_url(
            bucket=resource.bucket, object_uid=resource.uid
        )
        resources[resource.uid] = ResourceUrl(uid=resource.uid, url=download_url)
    return list(resources.values())


def is_valid_task_status_transition(old_status: TaskState, new_status: TaskState):
    if old_status is TaskState.SPAWNED and new_status in (
        TaskState.STARTED,
        TaskState.FINISHED,
        TaskState.CRASHED,
    ):
        return True
    if old_status is TaskState.STARTED and new_status in (
        TaskState.FINISHED,
        TaskState.CRASHED,
    ):
        return True
    return False
