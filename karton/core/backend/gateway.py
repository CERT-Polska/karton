import contextlib
import logging
import time
import urllib.parse
from collections import defaultdict
from collections.abc import Callable, Iterator
from io import BytesIO
from typing import IO, Any, Literal, cast

import httpx2
from pydantic import TypeAdapter

from karton.core.config import Config
from karton.core.exceptions import BindExpiredError
from karton.core.gateway_client import (
    AsyncGatewayClient,
    ClientConnection,
    GatewayBindExpiredError,
    OperationTimeoutError,
    SyncGatewayClient,
)
from karton.core.gateway_inspect_protocol import (
    AnalysisView,
    BatchTaskRequest,
    BindView,
    BulkRestartedTask,
    MetricsView,
    ProducerOutput,
    QueueView,
    RestartedTask,
    ServiceInfoView,
    TaskView,
)
from karton.core.gateway_protocol import (
    BindRequest,
    BindRequestMessage,
    BindResponse,
    DeclareTaskRequest,
    DeclareTaskRequestMessage,
    GetTaskRequest,
    GetTaskRequestMessage,
    HelloRequest,
    HelloRequestMessage,
    HelloResponse,
    LogResponse,
    LogSentResponse,
    NewTaskParameters,
    ResourceUrl,
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
from karton.core.resource import (
    LocalResource,
    LocalResourceBase,
    RemoteResource,
    ResourceBase,
    ResourceData,
)
from karton.core.task import (
    Task,
    TaskPriority,
    TaskState,
    map_resource_data,
    root_uid_from_task_uid,
)
from karton.core.utils import chunks, recursive_map

from .base import (
    KartonBackendProtocol,
    KartonBind,
    KartonExternalServiceInfo,
    KartonMetrics,
    KartonOutputs,
    KartonQueueState,
    KartonServiceInfo,
)

logger = logging.getLogger(__name__)

type ResourceIdentifier = tuple[str | None, str]  # Tuple[bucket, uid]


class KartonGatewayBackendBase:
    def __init__(
        self,
        config: Config,
        service_info: KartonServiceInfo,
    ):
        self.config = config
        self.service_info = service_info

        self.gateway_base_url = self.config.get("gateway", "url")
        self.gateway_url, self.rest_url = self._derive_gateway_urls(
            self.gateway_base_url
        )
        self.gateway_s3_hostname_override = self.config.get(
            "gateway", "s3_hostname_override"
        )

        auth_tokens: list[str] = self.config.get("gateway", "auth_tokens", "").split(
            ","
        )

        # In some scenarios is may be easier to roll tokens having
        # separate environment variable
        secondary_auth_tokens: list[str] = self.config.get(
            "gateway", "secondary_auth_tokens", ""
        ).split(",")

        # Filter out empty tokens
        self.gateway_auth_tokens: list[str] = [
            token for token in auth_tokens + secondary_auth_tokens if token
        ]

        self.gateway_retries = self.config.getint("gateway", "retries", 5)
        self.gateway_retry_base_timeout = self.config.getint(
            "gateway", "retry_base_timeout", 2
        )
        self.gateway_retry_jitter = self.config.getint("gateway", "retry_jitter", 1)
        self.gateway_connect_timeout = self.config.getint(
            "gateway", "connect_timeout", 5
        )
        self.gateway_response_timeout = self.config.getint(
            "gateway", "response_timeout", 15
        )
        self._bind_id: str | None = None

    @staticmethod
    def _derive_gateway_urls(gateway_url: str) -> tuple[str, str]:
        parsed = urllib.parse.urlparse(gateway_url)
        if parsed.scheme == "http":
            ws_scheme = "ws"
        elif parsed.scheme == "https":
            ws_scheme = "wss"
        else:
            raise ValueError(
                f"Gateway URL must start with http or https scheme (found '{parsed.scheme}')."
            )
        path = parsed.path

        rest_url = parsed._replace(path=path.rstrip("/") + "/api/v1")
        ws_url = parsed._replace(scheme=ws_scheme, path=path.rstrip("/") + "/gateway")

        return urllib.parse.urlunparse(ws_url), urllib.parse.urlunparse(rest_url)

    def _validate_local_resources(
        self, resources: dict[ResourceIdentifier, LocalResourceBase]
    ) -> None:
        """
        Validates that local resources don't have foreign buckets set.

        Karton Gateway only allows uploads to the default bucket (managed
        by the gateway server). Resources with bucket=None are fine (the
        server resolves them to the default bucket), but explicit non-None
        buckets are rejected client-side to fail fast before contacting the
        server.

        :param resources: Local resources collected by serialize_resources
        """
        for resource in resources.values():
            if resource.bucket is not None:
                raise RuntimeError(
                    f"Karton Gateway backend doesn't allow uploading "
                    f"resources to custom buckets (resource '{resource.uid}' "
                    f"has bucket '{resource.bucket}' set). Leave bucket unset "
                    f"to use the default bucket."
                )

    async def session_initiator_callback(
        self,
        gateway_client: AsyncGatewayClient,
        connection: ClientConnection,
        close_on_idle: bool,
    ) -> None:
        """
        Initiate new connection by receiving the server banner and authenticating
        """
        await gateway_client.recv(connection, expected_response=HelloResponse)
        hello_request = HelloRequest(
            message=HelloRequestMessage(
                identity=self.service_info.identity,
                service_version=self.service_info.service_version,
                library_version=self.service_info.karton_version,
                instance_id=self.service_info.instance_id,
                debug=self.service_info.debug,
                auth_tokens=self.gateway_auth_tokens,
                close_on_idle=close_on_idle,
            )
        )
        await gateway_client.send(connection, hello_request)
        await gateway_client.recv(connection, expected_response=SuccessResponse)


def override_presigned_url(presigned_url: str, override_host: str) -> tuple[str, str]:
    """
    Overrides presigned URL with new hostname.
    Returns URL netloc (to be put in Host header) and new URL
    """
    parsed_url = urllib.parse.urlparse(presigned_url)
    url_host = parsed_url.netloc
    return url_host, parsed_url._replace(netloc=override_host).geturl()


def serialize_resources(
    payload: dict[str, Any],
) -> tuple[dict[str, Any], dict[ResourceIdentifier, LocalResourceBase]]:
    local_resources: dict[ResourceIdentifier, LocalResourceBase] = {}

    def serialize_resource(obj: Any) -> Any:
        if isinstance(obj, ResourceBase):
            if to_upload := isinstance(obj, LocalResourceBase):
                local_resources[(obj.bucket, obj.uid)] = obj
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
    resource_deserializer: Callable[[ResourceData], ResourceBase],
) -> dict[str, Any]:
    deserialized_resources: dict[ResourceIdentifier, ResourceBase] = {}

    def deserialize_resource(obj: Any) -> Any:
        if type(obj) is dict and "__karton_resource__" in obj:
            resource = resource_deserializer(obj["__karton_resource__"])
            resource_identifier = (resource.bucket, resource.uid)
            if resource_identifier not in deserialized_resources:
                deserialized_resources[resource_identifier] = resource
            return deserialized_resources[resource_identifier]
        return obj

    return recursive_map(deserialize_resource, payload)


class KartonGatewayBackend(KartonGatewayBackendBase, KartonBackendProtocol):
    def __init__(
        self,
        config: Config,
        service_info: KartonServiceInfo,
    ) -> None:
        super().__init__(config, service_info)

        self._gateway_client = SyncGatewayClient(
            url=self.gateway_url,
            session_initiator_callback=self.session_initiator_callback,
            retries=self.gateway_retries,
            retry_base_timeout=self.gateway_retry_base_timeout,
            retry_jitter=self.gateway_retry_jitter,
            connect_timeout=self.gateway_connect_timeout,
            response_timeout=self.gateway_response_timeout,
            connection_pool_soft_limit=1,
        )

        auth_tokens = [t for t in self.gateway_auth_tokens if t]
        headers = {}
        if auth_tokens:
            headers["Authorization"] = f"Bearer {','.join(auth_tokens)}"
        self._http_client = httpx2.Client(
            base_url=self.rest_url,
            headers=headers,
            timeout=self.gateway_response_timeout,
        )

    def close(self) -> None:
        self._http_client.close()
        self._gateway_client.close()

    def register_bind(self, bind: KartonBind) -> KartonBind | None:
        response = self._gateway_client.make_request(
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

    def declare_task(self, task: Task) -> None:
        # Serialize resources
        payload, resources = serialize_resources(task.payload)
        payload_persistent, resources_persistent = serialize_resources(
            task.payload_persistent
        )
        resources.update(resources_persistent)
        self._validate_local_resources(resources)
        response = self._gateway_client.make_request(
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
        # Ignore bucket part: resources can be uploaded only
        # to the default Karton bucket
        resources_by_uid = {uid: res for (_, uid), res in resources.items()}
        for upload_url in response.message.upload_urls:
            resources_by_uid[upload_url.uid].bind_upload_url(upload_url.url)

    def set_task_status(self, task: Task, status: TaskState) -> None:
        self._gateway_client.make_request(
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

    def produce_unrouted_task(self, task: Task) -> None:
        self._gateway_client.make_request(
            request=SendTaskRequest(
                message=SendTaskRequestMessage(
                    token=cast(str, task.token),
                )
            ),
            expected_response=SuccessResponse,
        )

    def _deserialize_resources[T](
        self, payload: T, download_urls: list[ResourceUrl]
    ) -> T:
        grouped_download_urls: dict[ResourceIdentifier, str] = {
            (spec.bucket, spec.uid): spec.url for spec in download_urls
        }

        def deserialize_resource(resource_data: ResourceData) -> RemoteResource:
            return RemoteResource.from_dict(
                resource_data,
                backend=self,
                download_url=grouped_download_urls.get(
                    (resource_data["bucket"], resource_data["uid"]), None
                ),
            )

        return map_resource_data(payload, mapper=deserialize_resource)

    def consume_routed_task(self, identity: str, timeout: int = 5) -> Task | None:
        if self._bind_id is None:
            raise RuntimeError("Bug: Tried to consume task without registering bind")
        try:
            response = self._gateway_client.make_request(
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

        task_data = response.message.task
        payload, payload_persistent = self._deserialize_resources(
            (task_data.payload, task_data.payload_persistent),
            response.message.download_urls,
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

    def upload_resource(
        self, resource: LocalResource, content: bytes | IO[bytes]
    ) -> None:
        if isinstance(content, bytes):
            content = BytesIO(content)

        def streamer():
            while data := content.read(32768):
                yield data

        headers = {"Content-Length": str(resource.size)}

        if self.gateway_s3_hostname_override is not None:
            host, upload_url = override_presigned_url(
                resource.upload_url, self.gateway_s3_hostname_override
            )
            response = httpx2.put(
                upload_url, content=streamer(), headers={**headers, "Host": host}
            )
        else:
            response = httpx2.put(
                resource.upload_url, content=streamer(), headers=headers
            )

        response.raise_for_status()

    def upload_resource_from_file(self, resource: LocalResource, path: str) -> None:
        with open(path, "rb") as f:
            self.upload_resource(resource, f)

    def _get_download_stream(
        self, resource: RemoteResource
    ) -> contextlib.AbstractContextManager[httpx2.Response]:
        timeout = self.gateway_response_timeout
        if self.gateway_s3_hostname_override is not None:
            host, download_url = override_presigned_url(
                resource.download_url, self.gateway_s3_hostname_override
            )
            return httpx2.stream(
                "GET", download_url, headers={"Host": host}, timeout=timeout
            )
        else:
            return httpx2.stream("GET", resource.download_url, timeout=timeout)

    def iter_download_resource(
        self, resource: RemoteResource, chunk_size: int = 64 * 1024
    ) -> Iterator[bytes]:
        with self._get_download_stream(resource) as response:
            response.raise_for_status()
            yield from response.iter_bytes(chunk_size)

    def download_resource(self, resource: RemoteResource) -> bytes:
        with self._get_download_stream(resource) as response:
            response.raise_for_status()
            return response.read()

    def download_resource_to_file(self, resource: RemoteResource, path: str) -> None:
        with self._get_download_stream(resource) as response:
            response.raise_for_status()
            with open(path, "wb") as f:
                f.writelines(response.iter_bytes())

    def produce_log(
        self, log_record: dict[str, Any], logger_name: str, level: str
    ) -> bool:
        status = self._gateway_client.make_request(
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

    def consume_log(
        self,
        timeout: int = 5,
        logger_filter: str | None = None,
        level: str | None = None,
    ) -> Iterator[dict[str, Any] | None]:
        for log_response in self._gateway_client.make_streaming_request(
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
            else:
                # return control back to the caller in case a shutdown or some
                # other action was requested and needs to be handled
                yield None

    def increment_metrics(self, metric: KartonMetrics, identity: str) -> None:
        # This is no-op, Karton gateway manages all metrics
        return

    def get_all_metrics(self) -> dict[KartonMetrics, dict[str, int]]:
        response = self._http_client.get("/metrics")
        response.raise_for_status()
        return MetricsView.model_validate_json(response.content).metrics

    def _task_from_view(self, view: TaskView) -> Task:
        payload, payload_persistent = self._deserialize_resources(
            (view.payload, view.payload_persistent), view.download_urls
        )

        return Task(
            headers=view.headers,
            headers_persistent=view.headers_persistent,
            payload=payload,
            payload_persistent=payload_persistent,
            priority=TaskPriority(view.priority),
            parent_uid=view.parent_uid,
            root_uid=view.root_uid,
            orig_uid=view.orig_uid,
            uid=view.uid,
            error=view.error,
            _status=TaskState(view.status),
            _last_update=view.last_update,
        )

    @staticmethod
    def _bind_from_view(view: BindView) -> KartonBind:
        return KartonBind(**view.model_dump())

    @staticmethod
    def _service_info_from_view(
        view: ServiceInfoView,
    ) -> list[KartonExternalServiceInfo]:
        return [
            KartonExternalServiceInfo(
                identity=view.identity,
                karton_version=view.karton_version,
                service_version=view.service_version,
            )
            for _ in range(view.active_replicas)
        ]

    def get_binds(self) -> list[KartonBind]:
        response = self._http_client.get("/binds")
        response.raise_for_status()
        return [
            self._bind_from_view(b)
            for b in TypeAdapter(list[BindView]).validate_json(response.content)
        ]

    def get_bind(self, identity: str) -> KartonBind | None:
        response = self._http_client.get(f"/binds/{identity}")
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return self._bind_from_view(BindView.model_validate_json(response.content))

    def get_online_services(self) -> list[KartonExternalServiceInfo]:
        response = self._http_client.get("/services")
        response.raise_for_status()
        services: list[KartonExternalServiceInfo] = []
        for s in TypeAdapter(list[ServiceInfoView]).validate_json(response.content):
            services.extend(self._service_info_from_view(s))
        return services

    def get_online_identities(self) -> dict[str, list[KartonExternalServiceInfo]]:
        response = self._http_client.get("/services")
        response.raise_for_status()
        identities: dict[str, list[KartonExternalServiceInfo]] = defaultdict(list)
        for s in TypeAdapter(list[ServiceInfoView]).validate_json(response.content):
            identities[s.identity].extend(self._service_info_from_view(s))
        return dict(identities)

    def get_queues(self, parse_resources: bool = False) -> dict[str, KartonQueueState]:
        response = self._http_client.get("/queues")
        response.raise_for_status()
        raw = TypeAdapter(dict[str, QueueView]).validate_json(response.content)
        return {
            identity: KartonQueueState(
                bind=KartonBind(
                    identity=qv.identity,
                    info=qv.info,
                    version=qv.version,
                    persistent=qv.persistent,
                    filters=qv.filters,
                    service_version=qv.service_version,
                    is_async=qv.is_async,
                ),
                replicas=qv.replicas,
                pending_task_uids=qv.pending_task_uids,
                crashed_task_uids=qv.crashed_task_uids,
            )
            for identity, qv in raw.items()
        }

    def get_tasks(self, task_uids: list[str]) -> list[Task]:
        if not task_uids:
            return []
        tasks: list[Task] = []
        for chunk in chunks(task_uids, 10000):
            response = self._http_client.post(
                "/tasks/batch",
                json=BatchTaskRequest(uids=list(chunk)).model_dump(),
            )
            response.raise_for_status()
            views = TypeAdapter(list[TaskView]).validate_json(response.content)
            tasks.extend(self._task_from_view(view) for view in views)
        return tasks

    def get_analysis_tasks(self, root_uid: str) -> dict[str, list[Task]]:
        response = self._http_client.get(f"/analyses/{root_uid}")
        response.raise_for_status()
        view = AnalysisView.model_validate_json(response.content)
        return {
            receiver: [self._task_from_view(t) for t in tasks]
            for receiver, tasks in view.queues.items()
        }

    def get_task(self, task_uid: str) -> Task | None:
        response = self._http_client.get(f"/tasks/{task_uid}")
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return self._task_from_view(TaskView.model_validate_json(response.content))

    def restart_task(self, task: Task) -> Task:
        response = self._http_client.post(f"/tasks/{task.uid}/restart")
        response.raise_for_status()
        new_uid = RestartedTask.model_validate_json(response.content).uid
        new_task = self.get_task(new_uid)
        if new_task is None:
            raise RuntimeError(f"Restarted task {new_uid} not found")
        return new_task

    def cancel_task(self, task: Task) -> None:
        response = self._http_client.post(f"/tasks/{task.uid}/cancel")
        response.raise_for_status()

    def restart_queue_crashed_tasks(
        self,
        identity: str,
    ) -> list[tuple[str, str]]:
        response = self._http_client.post(
            f"/queues/{identity}/restart",
        )
        response.raise_for_status()
        restarted = TypeAdapter(list[BulkRestartedTask]).validate_json(response.content)
        return [(r.old_uid, r.new_uid) for r in restarted]

    def cancel_queue_tasks(
        self,
        identity: str,
        status: Literal["crashed", "pending"] = "crashed",
    ) -> None:
        response = self._http_client.post(
            f"/queues/{identity}/cancel",
            params={"status": status},
        )
        response.raise_for_status()

    def get_outputs(self) -> list[KartonOutputs]:
        response = self._http_client.get("/outputs")
        response.raise_for_status()
        outputs = TypeAdapter(list[ProducerOutput]).validate_json(response.content)
        return [KartonOutputs(identity=o.identity, outputs=o.outputs) for o in outputs]
