import dataclasses
import datetime
import enum
import uuid
from typing import Any, Callable, Iterator, TypedDict

import jwt
from pydantic import ValidationError

from karton.core.resource import ResourceBase
from karton.core.task import Task, TaskState
from karton.gateway.backend import gateway_backend
from karton.gateway.errors import InvalidTaskError, InvalidTaskTokenError
from karton.gateway.models import (
    DeclaredResourceSpec,
    ResourceUrl,
    ValidatedDeclaredResourceSpec,
)


class TaskTokenScope(enum.Enum):
    declared_task = "declared_task"
    consumed_task = "consumed_task"


class AllowedResource(TypedDict):
    uid: str
    bucket: str


@dataclasses.dataclass
class TaskTokenInfo:
    task_uid: str
    resources: list[AllowedResource]


PayloadBags = tuple[dict[str, Any], dict[str, Any]]


def parse_task_token(
    token: str, secret_key: str, audience: str, scope: TaskTokenScope | None
) -> TaskTokenInfo:
    try:
        token_data = jwt.decode(
            token,
            secret_key,
            algorithms=["HS256"],
            audience=[audience],
            options={"require": ["exp", "iss", "sub", "scope", "resources"]},
        )
        if not token_data["sub"].startswith("karton.task:"):
            raise jwt.exceptions.InvalidSubjectError(
                "Subject of this token is not a karton.task"
            )
        if scope is not None and scope is not TaskTokenScope(token_data["scope"]):
            raise InvalidTaskTokenError(
                f"Invalid task token: expected scope '{scope.value}', "
                f"got '{token_data['scope']}'"
            )
        task_uid = token_data["sub"][len("karton.task:") :]
        return TaskTokenInfo(task_uid=task_uid, resources=token_data["resources"])
    except jwt.InvalidTokenError as e:
        raise InvalidTaskTokenError(f"Invalid task token: {type(e)} - {str(e)}")


def make_task_token(
    task_token_info: TaskTokenInfo,
    secret_key: str,
    audience: str,
    scope: TaskTokenScope,
) -> str:
    issued_at = datetime.datetime.now(datetime.timezone.utc)
    payload = {
        "sub": f"karton.task:{task_token_info.task_uid}",
        "exp": issued_at + datetime.timedelta(days=1),
        "iat": issued_at,
        "iss": "karton.gateway",
        "aud": audience,
        "resources": task_token_info.resources,
        "scope": scope.value,
    }
    return jwt.encode(payload, secret_key, algorithm="HS256")


def iter_resources(obj: Any) -> Iterator[dict[str, Any]]:
    if type(obj) is dict:
        if "__karton_resource__" in obj:
            yield obj["__karton_resource__"]
        else:
            for v in obj.values():
                yield from iter_resources(v)
    elif type(obj) in (list, tuple):
        for el in obj:
            yield from iter_resources(el)


def map_resources(obj: Any, mapper: Callable[[dict[str, Any]], Any]) -> Any:
    if type(obj) is dict:
        if "__karton_resource__" in obj:
            return mapper(obj["__karton_resource__"])
        else:
            return {k: map_resources(v, mapper) for k, v in obj.items()}
    elif type(obj) in (list, tuple):
        return [map_resources(el, mapper) for el in obj]
    else:
        return obj


def is_resource_allowed(
    uid: str, bucket: str, allowed_parent_resources: list[AllowedResource]
):
    return {"uid": uid, "bucket": bucket} in allowed_parent_resources


def process_declared_task_resources(
    payload_bags: PayloadBags,
    allowed_parent_resources: list[AllowedResource],
    allowed_foreign_buckets: list[str],
) -> tuple[PayloadBags, list[ValidatedDeclaredResourceSpec]]:
    """
    Performs a lookup for resources referenced in payload bags, validates them
    and maps them to ResourceBase objects for Task.serialize

    ResourceBase objects are the representation of resources that is exchanged
    via Redis.

    :param payload_bags: Payload bags to process
    :param allowed_parent_resources: Allowed remote resources referenced in parent token
    :param allowed_foreign_buckets: Allowed foreign buckets to use for resource upload
    :return: |
        Tuple with two elements: payload bags with mapped resources to ResourceBase and
        list of DeclaredResourceSpec objects with validated resource specification.
    """
    resources: dict[tuple[str, str], ValidatedDeclaredResourceSpec] = {}

    def process_resource(resource_data: dict[str, Any]) -> Any:
        try:
            resource_spec = DeclaredResourceSpec.model_validate(resource_data)
        except ValidationError as e:
            raise InvalidTaskError(f"Invalid resource specification {e}")

        resource_server_uid: str
        resource_bucket: str

        if (
            resource_spec.bucket is None
            or resource_spec.bucket == gateway_backend.default_bucket_name
        ):
            # If bucket is set to None: service wants to use Karton-managed bucket
            resource_bucket = gateway_backend.default_bucket_name
            if not resource_spec.to_upload:
                # to_upload=False is passthrough of RemoteResource reference
                # Service must be authorized to reference the resource
                if not is_resource_allowed(
                    resource_spec.uid, resource_bucket, allowed_parent_resources
                ):
                    raise InvalidTaskError(
                        f"Service is not allowed to reference resource "
                        f"'{resource_spec.uid}'"
                    )
                resource_server_uid = resource_spec.uid
            else:
                # to_upload=True is LocalResource. In that case we don't trust
                # the UID from the client - we treat is as a payload bag reference
                # and we generate real UID server-side
                resource_server_uid = str(uuid.uuid4())
        else:
            # If bucket is not set to None or default bucket
            # then it's a foreign bucket reference
            if resource_spec.to_upload:
                # We don't allow foreign bucket uploads
                raise InvalidTaskError(
                    f"Service is not allowed to upload resource "
                    f"'{resource_spec.uid}' "
                    f"to foreign bucket '{resource_spec.bucket}'"
                )
            if (
                not is_resource_allowed(
                    resource_spec.uid, resource_spec.bucket, allowed_parent_resources
                )
                and resource_spec.bucket not in allowed_foreign_buckets
            ):
                # Foreign bucket downloads are allowed only:
                # - if object was received from parent task
                # OR
                # - if service is allowed to reference a foreign bucket
                raise InvalidTaskError(
                    f"Service is not allowed to reference "
                    f"bucket '{resource_spec.bucket}' "
                    f"in resource '{resource_spec.uid}'"
                )
            resource_server_uid = resource_spec.uid
            resource_bucket = resource_spec.bucket

        resource_identity = (resource_bucket, resource_spec.uid)

        if resource_identity not in resources:
            validated_spec = ValidatedDeclaredResourceSpec(
                uid=resource_spec.uid,
                name=resource_spec.name,
                size=resource_spec.size,
                metadata=resource_spec.metadata,
                sha256=resource_spec.sha256,
                to_upload=resource_spec.to_upload,
                bucket=resource_bucket,
                server_uid=resource_server_uid,
            )
            resources[resource_identity] = validated_spec
        else:
            validated_spec = resources[resource_identity]

        return ResourceBase(
            _uid=validated_spec.server_uid,
            _size=validated_spec.size,
            name=validated_spec.name,
            metadata=validated_spec.metadata,
            sha256=validated_spec.sha256,
            bucket=validated_spec.bucket,
        )

    transformed_payload_bags = map_resources(payload_bags, process_resource)
    return transformed_payload_bags, list(resources.values())


async def generate_resource_upload_urls(
    resources: list[ValidatedDeclaredResourceSpec],
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
            upload_url = await gateway_backend.get_presigned_object_upload_url(
                bucket=bucket, object_uid=resource.server_uid
            )
            upload_urls.append(
                ResourceUrl(
                    uid=resource.uid,
                    bucket=bucket,
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
        if resource.bucket is None:
            # If bucket is somehow not specified, assume default bucket
            bucket = gateway_backend.default_bucket_name
        else:
            bucket = resource.bucket
        if (bucket, resource.uid) in resources:
            continue
        if bucket not in allowed_buckets:
            raise InvalidTaskError(
                f"Got task that references bucket '{bucket}' "
                f"that can't be handled by Karton Gateway"
            )
        download_url = await gateway_backend.get_presigned_object_download_url(
            bucket=bucket, object_uid=resource.uid
        )
        resources[(bucket, resource.uid)] = ResourceUrl(
            uid=resource.uid, bucket=bucket, url=download_url
        )
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
    if old_status is TaskState.DECLARED and new_status is TaskState.FINISHED:
        return True
    return False
