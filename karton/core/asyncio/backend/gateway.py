import contextlib
import logging
import time
from io import BytesIO
from typing import IO, Any, AsyncIterator, cast

import httpx2

from karton.core.asyncio.resource import LocalResource, RemoteResource
from karton.core.backend import KartonBind, KartonMetrics, KartonServiceInfo
from karton.core.backend.gateway import (
    KartonGatewayBackendBase,
    ResourceIdentifier,
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
from karton.core.gateway_protocol import (
    BindRequest,
    BindRequestMessage,
    BindResponse,
    DeclareTaskRequest,
    DeclareTaskRequestMessage,
    GetTaskRequest,
    GetTaskRequestMessage,
    LogResponse,
    LogSentResponse,
    NewTaskParameters,
    SendLogRequest,
    SendLogRequestMessage,
    SendTaskRequest,
    SendTaskRequestMessage,
    SetTaskStatusRequest,
    SetTaskStatusRequestMessage,
    SubscribeLogsRequest,
    SubscribeLogsRequestMessage,
    SuccessResponse,
    TaskDeclaredResponse,
    TaskResponse,
)
from karton.core.task import Task, TaskPriority, TaskState, root_uid_from_task_uid

from .base import KartonAsyncBackendProtocol

logger = logging.getLogger(__name__)


class KartonAsyncGatewayBackend(KartonGatewayBackendBase, KartonAsyncBackendProtocol):
    def __init__(
        self,
        config: Config,
        identity: str | None = None,
        service_info: KartonServiceInfo | None = None,
    ) -> None:
        super().__init__(config, identity, service_info)
        self.connection_pool_soft_limit = self.config.getint(
            "gateway", "connection_pool_soft_limit", 4
        )
        self._gateway_client = AsyncGatewayClient(
            url=self.gateway_url,
            session_initiator_callback=self.session_initiator_callback,
            retries=self.gateway_retries,
            retry_base_timeout=self.gateway_retry_base_timeout,
            retry_jitter=self.gateway_retry_jitter,
            connect_timeout=self.gateway_connect_timeout,
            response_timeout=self.gateway_response_timeout,
            connection_pool_soft_limit=self.connection_pool_soft_limit,
        )
        self._http_client = httpx2.AsyncClient()

    async def connect(self) -> None:
        pass

    async def close(self) -> None:
        await self._http_client.aclose()
        await self._gateway_client.close()

    async def register_bind(self, bind: KartonBind) -> KartonBind | None:
        response = await self._gateway_client.make_request(
            request=BindRequest(
                message=BindRequestMessage(
                    info=bind.info,
                    filters=bind.filters,
                    persistent=bind.persistent,
                    is_async=bind.is_async,
                )
            ),
            expected_response=BindResponse,
        )
        self._bind_id = response.message.bind_id
        return response.message.old_bind

    async def declare_task(self, task: Task) -> None:
        # Serialize resources
        payload, resources = serialize_resources(task.payload)
        payload_persistent, resources_persistent = serialize_resources(
            task.payload_persistent
        )
        resources.update(resources_persistent)
        response = await self._gateway_client.make_request(
            request=DeclareTaskRequest(
                message=DeclareTaskRequestMessage(
                    task=NewTaskParameters(
                        headers=task.headers,
                        headers_persistent=task.headers_persistent,
                        payload=payload,
                        payload_persistent=payload_persistent,
                        priority=task.priority,
                    ),
                    parent_token=task.parent_token,
                )
            ),
            expected_response=TaskDeclaredResponse,
        )
        task.uid = response.message.uid
        task.root_uid = root_uid_from_task_uid(response.message.uid)
        task.bind_token(response.message.token)
        for upload_url in response.message.upload_urls:
            resources[(upload_url.bucket, upload_url.uid)].bind_upload_url(
                upload_url.url
            )

    async def set_task_status(self, task: Task, status: TaskState) -> None:
        if task.status == status:
            return
        await self._gateway_client.make_request(
            request=SetTaskStatusRequest(
                message=SetTaskStatusRequestMessage(
                    token=cast(str, task.token),
                    status=status,
                    error=task.error,
                )
            ),
            expected_response=SuccessResponse,
        )
        task.status = status
        task.last_update = time.time()

    async def produce_unrouted_task(self, task: Task) -> None:
        await self._gateway_client.make_request(
            request=SendTaskRequest(
                message=SendTaskRequestMessage(
                    token=cast(str, task.token),
                )
            ),
            expected_response=SuccessResponse,
        )

    async def consume_routed_task(self, identity: str, timeout: int = 5) -> Task | None:
        if self._bind_id is None:
            raise RuntimeError("Bug: Tried to consume task without registering bind")
        try:
            response = await self._gateway_client.make_request(
                request=GetTaskRequest(
                    message=GetTaskRequestMessage(
                        bind_id=self._bind_id,
                    )
                ),
                expected_response=TaskResponse,
            )
        except OperationTimeoutError:
            return None
        except GatewayBindExpiredError as e:
            raise BindExpiredError(e.message) from e
        download_urls: dict[ResourceIdentifier, str] = {
            (spec.bucket, spec.uid): spec.url for spec in response.message.download_urls
        }

        def deserialize_resource(resource_data: dict[str, Any]) -> RemoteResource:
            return RemoteResource.from_dict(
                resource_data,
                backend=self,
                download_url=download_urls[
                    (resource_data["bucket"], resource_data["uid"])
                ],
            )

        task_data = response.message.task
        payload = deserialize_resources(task_data.payload, deserialize_resource)
        payload_persistent = deserialize_resources(
            task_data.payload_persistent, deserialize_resource
        )

        return Task(
            uid=task_data.uid,
            root_uid=root_uid_from_task_uid(task_data.uid),
            parent_uid=task_data.parent_uid,
            orig_uid=task_data.orig_uid,
            headers=task_data.headers,
            headers_persistent=task_data.headers_persistent,
            payload=payload,
            payload_persistent=payload_persistent,
            priority=TaskPriority(task_data.priority),
            _status=TaskState.SPAWNED,
            _token=response.message.token,
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
    ) -> contextlib.AbstractAsyncContextManager[httpx2.Response]:
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
            request=SendLogRequest(
                message=SendLogRequestMessage(
                    log_record=log_record,
                    logger_name=logger_name,
                    level=level,
                ),
            ),
            expected_response=LogSentResponse,
        )
        return status.message.was_received

    async def consume_log(
        self,
        timeout: int = 5,
        logger_filter: str | None = None,
        level: str | None = None,
    ) -> AsyncIterator[dict[str, Any] | None]:
        async for log_response in self._gateway_client.make_streaming_request(
            request=SubscribeLogsRequest(
                message=SubscribeLogsRequestMessage(
                    logger_filter=logger_filter,
                    level=level,
                ),
            ),
            expected_response=LogResponse,
        ):
            if log_response.message.log_record:
                yield log_response.message.log_record

    async def increment_metrics(self, metric: KartonMetrics, identity: str) -> None:
        # This is no-op, Karton gateway manages all metrics
        return
