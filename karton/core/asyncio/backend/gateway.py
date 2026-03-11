import contextlib
import logging
import time
from io import BytesIO
from typing import IO, Any, AsyncIterator

import httpx

from karton.core.asyncio.resource import LocalResource, RemoteResource
from karton.core.backend import KartonBind, KartonMetrics, KartonServiceInfo
from karton.core.backend.base import unserialize_bind
from karton.core.backend.gateway import (
    KartonGatewayBackendBase,
    deserialize_resources,
    override_presigned_url,
    serialize_resources,
)
from karton.core.config import Config
from karton.core.exceptions import BindExpiredError
from karton.core.gateway_client import (
    AsyncGatewayClient,
    GatewayBindExpiredError,
    OperationTimeoutError,
)
from karton.core.task import Task, TaskPriority, TaskState, root_uid_from_task_uid

from .base import KartonAsyncBackendProtocol

logger = logging.getLogger(__name__)


class KartonGatewayBackend(KartonGatewayBackendBase, KartonAsyncBackendProtocol):
    def __init__(
        self,
        config: Config,
        identity: str | None = None,
        service_info: KartonServiceInfo | None = None,
    ):
        super().__init__(config, identity, service_info)
        self.gateway_connection_pool_soft_limit = self.config.getint(
            "gateway", "connection_pool_soft_limit", 4
        )
        self._gateway_client = AsyncGatewayClient(
            url=self.gateway_url,
            session_initiator=self._session_initiator,
            retries=self.gateway_retries,
            retry_base_timeout=self.gateway_retry_base_timeout,
            retry_jitter=self.gateway_retry_jitter,
            connect_timeout=self.gateway_connect_timeout,
            response_timeout=self.gateway_response_timeout,
            connection_pool_soft_limit=self.gateway_connection_pool_soft_limit,
        )
        self._http_client = httpx.AsyncClient()

    async def connect(self) -> None:
        pass

    async def register_bind(self, bind: KartonBind) -> KartonBind | None:
        response = await self._gateway_client.make_request(
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

    async def declare_task(self, task: Task) -> None:
        # Serialize resources
        payload, resources = serialize_resources(task.payload)
        payload_persistent, resources_persistent = serialize_resources(
            task.payload_persistent
        )
        resources.update(resources_persistent)
        response = await self._gateway_client.make_request(
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

    async def set_task_status(self, task: Task, status: TaskState) -> None:
        if task.status == status:
            return
        task.status = status
        task.last_update = time.time()
        message: dict[str, Any] = {"token": task.token, "status": status.value}
        if status is TaskState.CRASHED:
            message["error"] = task.error
        await self._gateway_client.make_request(
            request="set_task_status",
            message=message,
            expected_response="success",
        )

    async def produce_unrouted_task(self, task: Task) -> None:
        await self._gateway_client.make_request(
            request="send_task",
            message={
                "token": task.token,
            },
            expected_response="success",
        )

    async def consume_routed_task(self, identity: str, timeout: int = 5) -> Task | None:
        try:
            response = await self._gateway_client.make_request(
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

    async def upload_resource(
        self, resource: LocalResource, content: bytes | IO[bytes]
    ) -> None:
        if type(content) is bytes:
            content = BytesIO(content)

        async def streamer():
            while data := content.read(32768):
                yield data

        headers = {"Content-Length": str(resource.size)}

        if self.gateway_s3_hostname_override is not None:
            host, upload_url = override_presigned_url(
                resource.upload_url, self.gateway_s3_hostname_override
            )
            response = await self._http_client.put(
                upload_url, content=streamer(), headers={**headers, "Host": host}
            )
        else:
            response = await self._http_client.put(
                resource.upload_url, content=streamer(), headers=headers
            )

        response.raise_for_status()

    async def upload_resource_from_file(
        self, resource: LocalResource, path: str
    ) -> None:
        with open(path, "rb") as f:
            await self.upload_resource(resource, f)

    def _get_download_stream(
        self, resource: RemoteResource
    ) -> contextlib.AbstractAsyncContextManager[httpx.Response]:
        if self.gateway_s3_hostname_override is not None:
            host, download_url = override_presigned_url(
                resource.download_url, self.gateway_s3_hostname_override
            )
            return self._http_client.stream("GET", download_url, headers={"Host": host})
        else:
            return self._http_client.stream("GET", resource.download_url)

    async def download_resource(self, resource: RemoteResource) -> bytes:
        async with self._get_download_stream(resource) as response:
            response.raise_for_status()
            return await response.aread()

    async def download_resource_to_file(
        self, resource: RemoteResource, path: str
    ) -> None:
        async with self._get_download_stream(resource) as response:
            response.raise_for_status()
            with open(path, "wb") as f:
                async for chunk in response.aiter_bytes():
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

    async def produce_log(
        self, log_record: dict[str, Any], logger_name: str, level: str
    ) -> bool:
        status = await self._gateway_client.make_request(
            request="send_log",
            message={
                "log_record": log_record,
                "logger_name": logger_name,
                "level": level,
            },
            expected_response="log_sent",
        )
        return status["was_received"]

    async def consume_log(
        self,
        timeout: int = 5,
        logger_filter: str | None = None,
        level: str | None = None,
    ) -> AsyncIterator[dict[str, Any] | None]:
        async for log_response in self._gateway_client.make_streaming_request(
            request="subscribe_logs",
            message={
                "logger_filter": logger_filter,
                "level": level,
            },
            expected_response="log",
        ):
            yield log_response["log_record"]

    async def increment_metrics(self, metric: KartonMetrics, identity: str) -> None:
        # This is no-op, Karton gateway manages all metrics
        return
