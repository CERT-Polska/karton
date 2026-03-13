import contextlib
import logging
import time
import urllib.parse
from io import BytesIO
from typing import IO, Any, Callable, Iterator

import httpx

from karton.core.config import Config
from karton.core.exceptions import BindExpiredError
from karton.core.gateway_client import (
    AsyncGatewayClient,
    ClientConnection,
    GatewayBindExpiredError,
    OperationTimeoutError,
    SyncGatewayClient,
)
from karton.core.resource import (
    LocalResource,
    LocalResourceBase,
    RemoteResource,
    ResourceBase,
)
from karton.core.task import Task, TaskPriority, TaskState, root_uid_from_task_uid
from karton.core.utils import recursive_map

from .base import (
    KartonBackendProtocol,
    KartonBind,
    KartonMetrics,
    KartonServiceInfo,
    resolve_service_info,
    unserialize_bind,
)

logger = logging.getLogger(__name__)


class KartonGatewayBackendBase:
    def __init__(
        self,
        config: Config,
        identity: str | None = None,
        service_info: KartonServiceInfo | None = None,
    ):
        self.config = config
        service_info = resolve_service_info(identity, service_info)
        if service_info is None:
            raise RuntimeError(
                "Service identity can't be None while using Karton Gateway"
            )
        self.service_info: KartonServiceInfo = service_info

        self.gateway_url = self.config.get("gateway", "url")
        self.gateway_password = self.config.get("gateway", "password")
        self.gateway_s3_hostname_override = self.config.get(
            "gateway", "s3_hostname_override"
        )

        self.gateway_retries = self.config.getint("gateway", "retries", 5)
        self.gateway_retry_base_timeout = self.config.getint(
            "gateway", "retry_base_timeout", 2
        )
        self.gateway_retry_jitter = self.config.getint("gateway", "retry_jitter", 1)
        self.gateway_connect_timeout = self.config.getint(
            "gateway", "connect_timeout", 5
        )
        self.gateway_response_timeout = self.config.getint(
            "gateway", "response_timeout", 5
        )
        self._karton_bind: KartonBind | None = None

        async def _session_initiator(
            gateway_client: AsyncGatewayClient,
            connection: ClientConnection,
            secondary: bool,
        ):
            await gateway_client.recv(connection, expected_response="hello")
            await gateway_client.send(
                connection,
                request="hello",
                message={
                    "identity": self.service_info.identity,
                    "service_version": self.service_info.service_version,
                    "library_version": self.service_info.karton_version,
                    "instance_id": self.service_info.instance_id,
                    "password": self.gateway_password,
                },
            )
            if not secondary and self._karton_bind is not None:
                # If it's main connection and bind is already registered
                # we need to re-register it for new session.
                bind = self._karton_bind
                await gateway_client.send(
                    connection,
                    request="bind",
                    message={
                        "info": bind.info,
                        "filters": bind.filters,
                        "persistent": bind.persistent,
                        "is_async": bind.is_async,
                    },
                )
                await gateway_client.recv(
                    connection,
                    expected_response="bind",
                )
            await gateway_client.recv(connection)

        self._session_initiator = _session_initiator


def override_presigned_url(presigned_url: str, override_host: str) -> tuple[str, str]:
    """
    Overrides presigned URL with new hostname.
    Returns URL netloc (to be put in Host header) and new URL
    """
    parsed_url = urllib.parse.urlparse(presigned_url)
    url_host = parsed_url.netloc
    return url_host, parsed_url._replace(netloc=override_host).geturl()


def serialize_resources(
    payload: dict[str, Any]
) -> tuple[dict[str, Any], dict[str, LocalResourceBase]]:
    local_resources: dict[str, LocalResourceBase] = {}

    def serialize_resource(obj: Any) -> Any:
        if isinstance(obj, ResourceBase):
            if to_upload := isinstance(obj, LocalResourceBase):
                local_resources[obj.uid] = obj
            return {
                "__karton_resource__": {
                    "uid": obj.uid,
                    "name": obj.name,
                    "size": obj.size,
                    "metadata": obj.metadata,
                    "sha256": obj.sha256,
                    "bucket": obj.bucket,
                    "to_upload": to_upload,
                }
            }
        return obj

    return recursive_map(serialize_resource, payload), local_resources


def deserialize_resources(
    payload: dict[str, Any],
    resource_deserializer: Callable[[dict[str, Any]], ResourceBase],
) -> dict[str, Any]:
    def deserialize_resource(obj: Any) -> Any:
        if type(obj) is dict and obj.keys() == {"__karton_resource__"}:
            return resource_deserializer(obj["__karton_resource__"])
        return obj

    return recursive_map(deserialize_resource, payload)


class KartonGatewayBackend(KartonGatewayBackendBase, KartonBackendProtocol):
    def __init__(
        self,
        config: Config,
        identity: str | None = None,
        service_info: KartonServiceInfo | None = None,
    ):
        super().__init__(config, identity, service_info)

        self._gateway_client = SyncGatewayClient(
            url=self.gateway_url,
            session_initiator=self._session_initiator,
            retries=self.gateway_retries,
            retry_base_timeout=self.gateway_retry_base_timeout,
            retry_jitter=self.gateway_retry_jitter,
            connect_timeout=self.gateway_connect_timeout,
            response_timeout=self.gateway_response_timeout,
            connection_pool_soft_limit=1,
        )

    def register_bind(self, bind: KartonBind) -> KartonBind | None:
        response = self._gateway_client.make_request(
            request="bind",
            message={
                "info": bind.info,
                "filters": bind.filters,
                "persistent": bind.persistent,
                "is_async": bind.is_async,
            },
            expected_response="bind",
        )
        if response["old_bind"] is None:
            return None
        old_bind = response["old_bind"]
        return unserialize_bind(old_bind["identity"], old_bind)

    def declare_task(self, task: Task) -> None:
        # Serialize resources
        payload, resources = serialize_resources(task.payload)
        payload_persistent, resources_persistent = serialize_resources(
            task.payload_persistent
        )
        resources.update(resources_persistent)
        response = self._gateway_client.make_request(
            request="declare_task",
            message={
                "task": {
                    "headers": task.headers,
                    "headers_persistent": task.headers_persistent,
                    "payload": payload,
                    "payload_persistent": payload_persistent,
                    "priority": task.priority.value,
                },
                "parent_token": task.parent_token,
            },
            expected_response="task_declared",
        )
        task.uid = response["uid"]
        task.root_uid = root_uid_from_task_uid(response["uid"])
        task.bind_token(response["token"])
        for upload_url in response["upload_urls"]:
            resources[upload_url["uid"]].bind_upload_url(upload_url["url"])

    def set_task_status(self, task: Task, status: TaskState) -> None:
        if task.status == status:
            return
        task.status = status
        task.last_update = time.time()
        message: dict[str, Any] = {"token": task.token, "status": status.value}
        if status is TaskState.CRASHED:
            message["error"] = task.error
        self._gateway_client.make_request(
            request="set_task_status",
            message=message,
            expected_response="success",
        )

    def produce_unrouted_task(self, task: Task) -> None:
        self._gateway_client.make_request(
            request="send_task",
            message={
                "token": task.token,
            },
            expected_response="success",
        )

    def consume_routed_task(self, identity: str, timeout: int = 5) -> Task | None:
        try:
            response = self._gateway_client.make_request(
                request="get_task", message={}, expected_response="task"
            )
        except OperationTimeoutError:
            return None
        except GatewayBindExpiredError as e:
            raise BindExpiredError(e.message) from e
        download_urls = {spec["uid"]: spec["url"] for spec in response["download_urls"]}

        def deserialize_resource(resource_data: dict[str, Any]) -> RemoteResource:
            return RemoteResource.from_dict(
                resource_data,
                backend=self,
                download_url=download_urls[resource_data["uid"]],
            )

        task_data = response["task"]
        payload = deserialize_resources(task_data["payload"], deserialize_resource)
        payload_persistent = deserialize_resources(
            task_data["payload_persistent"], deserialize_resource
        )

        return Task(
            uid=task_data["uid"],
            root_uid=root_uid_from_task_uid(task_data["uid"]),
            parent_uid=task_data["parent_uid"],
            orig_uid=task_data["orig_uid"],
            headers=task_data["headers"],
            headers_persistent=task_data["headers_persistent"],
            payload=payload,
            payload_persistent=payload_persistent,
            priority=TaskPriority(task_data["priority"]),
            _status=TaskState.SPAWNED,
            _token=response["token"],
        )

    def upload_resource(
        self, resource: LocalResource, content: bytes | IO[bytes]
    ) -> None:
        if type(content) is bytes:
            content = BytesIO(content)

        def streamer():
            while data := content.read(32768):
                yield data

        headers = {"Content-Length": str(resource.size)}

        if self.gateway_s3_hostname_override is not None:
            host, upload_url = override_presigned_url(
                resource.upload_url, self.gateway_s3_hostname_override
            )
            response = httpx.put(
                upload_url, content=streamer(), headers={**headers, "Host": host}
            )
        else:
            response = httpx.put(
                resource.upload_url, content=streamer(), headers=headers
            )

        response.raise_for_status()

    def upload_resource_from_file(self, resource: LocalResource, path: str) -> None:
        with open(path, "rb") as f:
            self.upload_resource(resource, f)

    def _get_download_stream(
        self, resource: RemoteResource
    ) -> contextlib.AbstractContextManager[httpx.Response]:
        if self.gateway_s3_hostname_override is not None:
            host, download_url = override_presigned_url(
                resource.download_url, self.gateway_s3_hostname_override
            )
            return httpx.stream("GET", download_url, headers={"Host": host})
        else:
            return httpx.stream("GET", resource.download_url)

    def download_resource(self, resource: RemoteResource) -> bytes:
        with self._get_download_stream(resource) as response:
            response.raise_for_status()
            return response.read()

    def download_resource_to_file(self, resource: RemoteResource, path: str) -> None:
        with self._get_download_stream(resource) as response:
            response.raise_for_status()
            with open(path, "wb") as f:
                for chunk in response.iter_bytes():
                    f.write(chunk)

    def upload_object(
        self,
        bucket: str,
        object_uid: str,
        content: bytes | IO[bytes],
    ) -> None:
        raise NotImplementedError(
            "Gateway backend doesn't allow to download arbitrary S3 objects"
            "KartonGatewayBackend.upload_resource should be used instead."
        )

    def upload_object_from_file(self, bucket: str, object_uid: str, path: str) -> None:
        raise NotImplementedError(
            "Gateway backend doesn't allow to download arbitrary S3 objects"
            "KartonGatewayBackend.upload_resource_from_file should be used instead."
        )

    def download_object(self, bucket: str, object_uid: str) -> bytes:
        raise NotImplementedError(
            "Gateway backend doesn't allow to download arbitrary S3 objects"
            "KartonGatewayBackend.download_resource should be used instead."
        )

    def download_object_to_file(self, bucket: str, object_uid: str, path: str) -> None:
        raise NotImplementedError(
            "Gateway backend doesn't allow to download arbitrary S3 objects"
            "KartonGatewayBackend.download_resource_to_file should be used instead."
        )

    def remove_object(self, bucket: str, object_uid: str) -> None:
        raise NotImplementedError(
            "Gateway backend doesn't allow to remove arbitrary S3 objects"
        )

    def produce_log(
        self, log_record: dict[str, Any], logger_name: str, level: str
    ) -> bool:
        status = self._gateway_client.make_request(
            request="send_log",
            message={
                "log_record": log_record,
                "logger_name": logger_name,
                "level": level,
            },
            expected_response="log_sent",
        )
        return status["was_received"]

    def consume_log(
        self,
        timeout: int = 5,
        logger_filter: str | None = None,
        level: str | None = None,
    ) -> Iterator[dict[str, Any] | None]:
        for log_response in self._gateway_client.make_streaming_request(
            request="subscribe_logs",
            message={
                "logger_filter": logger_filter,
                "level": level,
            },
            expected_response="log",
        ):
            yield log_response["log_record"]

    def increment_metrics(self, metric: KartonMetrics, identity: str) -> None:
        # This is no-op, Karton gateway manages all metrics
        return
