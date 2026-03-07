import contextlib
import json
import logging
import random
import time
import urllib.parse
from io import BytesIO
from typing import IO, Any, Callable, Iterator

import httpx
from websockets.protocol import CLOSED
from websockets.sync.client import ClientConnection, connect

from karton.core.config import Config
from karton.core.exceptions import BindExpiredError
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
        self.gateway_s3_hostname_override = self.config.get(
            "gateway", "s3_hostname_override"
        )

        self.gateway_retries = self.config.getint("gateway", "retries", 5)
        self.gateway_retry_base_timeout = self.config.getint(
            "gateway", "retry_base_timeout", 2
        )
        self.gateway_retry_jitter = self.config.getint("gateway", "retry_jitter", 1)
        self.gateway_response_timeout = self.config.getint(
            "gateway", "response_timeout", 5
        )


class GatewayError(Exception):
    code = ""

    def __init__(self, message: str, code: str | None = None):
        self.code = code or self.code
        self.message = message


class OperationTimeoutError(GatewayError):
    code = "timeout"


class GatewayBindExpiredError(GatewayError):
    code = "expired_bind"


class GatewayShutdownError(GatewayError):
    code = "shutdown_in_progress"


class GatewayHandshakeError(Exception):
    pass


def make_gateway_error(code: str, message: str) -> GatewayError:
    for error_class in GatewayError.__subclasses__():
        if error_class.code == code:
            return error_class(message)
    return GatewayError(code, message)


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
    resource_deserializer: Callable[[dict[str, Any]], RemoteResource],
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

        self._connection: ClientConnection | None = None
        self._karton_bind: KartonBind | None = None
        self._connection_used: bool = False

    def _recv(
        self,
        connection: ClientConnection,
        expected_response: str = "success",
        timeout: int = 10,
    ) -> dict[str, Any]:
        data = connection.recv(timeout=timeout)
        message = json.loads(data)
        if "response" not in message:
            raise RuntimeError("Incorrect gateway response, missing 'response' key")
        if message["response"] == "error":
            error = message["message"]
            code = error["code"]
            error_message = error["error_message"]
            raise make_gateway_error(code, error_message)
        if message["response"] != expected_response:
            raise RuntimeError(
                f"Got unexpected gateway response: {message['response']}, "
                f"expected {expected_response}"
            )
        return message["message"]

    def _send(
        self, connection: ClientConnection, request: str, message: dict[str, Any]
    ) -> None:
        data = json.dumps(
            {
                "request": request,
                "message": message,
            }
        )
        connection.send(data)

    def _init_connection(self, connection: ClientConnection) -> None:
        self._recv(
            connection, expected_response="hello", timeout=self.gateway_response_timeout
        )
        self._send(
            connection,
            request="hello",
            message={
                "identity": self.service_info.identity,
                "service_version": self.service_info.service_version,
                "library_version": self.service_info.karton_version,
                "instance_id": self.service_info.instance_id,
            },
        )
        self._recv(connection, timeout=self.gateway_response_timeout)

    def _connect(self, secondary=False) -> ClientConnection:
        _connection = connect(self.gateway_url)
        self._init_connection(_connection)
        if not secondary and self._karton_bind is not None:
            # If it's main connection and bind is already registered
            # we need to re-register it for new session.
            bind = self._karton_bind
            self._send(
                _connection,
                request="bind",
                message={
                    "info": bind.info,
                    "filters": bind.filters,
                    "persistent": bind.persistent,
                    "is_async": bind.is_async,
                },
            )
            self._recv(
                _connection,
                expected_response="bind",
                timeout=self.gateway_response_timeout,
            )
        return _connection

    @contextlib.contextmanager
    def _get_connection(self) -> Iterator[ClientConnection]:
        if self._connection_used:
            # There are corner cases where Karton tries to send command
            # while connection is used for handling another one.
            # One of them is self.log.info("Got signal, shutting down...")
            # that is called on CTRL-C during waiting in consume_routed_task
            # This causes ConcurrencyError. Let's initiate a secondary connection
            # in that case just for that operation.
            _connection = self._connect(secondary=True)
            try:
                yield _connection
                return
            finally:
                _connection.close()
        try:
            self._connection_used = True
            if self._connection is not None and self._connection.state is not CLOSED:
                yield self._connection
                return
            self._connection = self._connect()
            yield self._connection
            return
        except Exception:
            raise
        except BaseException:
            # On BaseException we forcefully close connection
            # to immediately interrupt blocking operations
            # (e.g. HardShutdownInterrupt during task recv)
            if self._connection:
                self._connection.close()
                self._connection = None
            raise
        finally:
            self._connection_used = False
            if self._connection is not None and self._connection.state is CLOSED:
                self._connection = None

    def _retry_delay(self, try_no):
        delay = self.gateway_retry_base_timeout * (2**try_no) + (
            (random.random() * 2 - 1) * self.gateway_retry_jitter
        )
        if delay <= 0:
            delay = self.gateway_retry_base_timeout
        logger.warning(
            "Failed to send request to gateway. Retry %d/%d after %.1f seconds",
            try_no + 1,
            self.gateway_retries,
            delay,
        )
        time.sleep(delay)

    def make_request(
        self, request: str, message: dict[str, Any], expected_response: str
    ):
        for try_no in range(self.gateway_retries + 1):
            request_sent = False
            try:
                with self._get_connection() as connection:
                    self._send(connection, request=request, message=message)
                    request_sent = True
                    return self._recv(
                        connection,
                        expected_response=expected_response,
                        timeout=self.gateway_response_timeout,
                    )
            except (ConnectionError, TimeoutError, GatewayShutdownError) as e:
                # Close the main connection, even if secondary connection
                # was the cause. We would need to reconnect to server anyway.
                if self._connection:
                    self._connection.close()
                if not isinstance(e, GatewayShutdownError) and request_sent:
                    # If request was successfully sent and we got
                    # ConnectionError/TimeoutError during waiting
                    # for an answer, we can't safely repeat it
                    # because request could be (partially)
                    # processed by gateway. The best we can do
                    # is to fail operation with an exception
                    raise
                if try_no == self.gateway_retries:
                    raise
            self._retry_delay(try_no)

    def register_bind(self, bind: KartonBind) -> KartonBind | None:
        response = self.make_request(
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
        response = self.make_request(
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
        self.make_request(
            request="set_task_status",
            message=message,
            expected_response="success",
        )

    def produce_unrouted_task(self, task: Task) -> None:
        self.make_request(
            request="send_task",
            message={
                "token": task.token,
            },
            expected_response="success",
        )

    def consume_routed_task(self, identity: str, timeout: int = 5) -> Task | None:
        try:
            response = self.make_request(
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
        status = self.make_request(
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
        try_no = 0
        while True:
            try:
                with self._get_connection() as connection:
                    self._send(
                        connection,
                        request="subscribe_logs",
                        message={
                            "logger_filter": logger_filter,
                            "level": level,
                        },
                    )
                    while True:
                        try:
                            log = self._recv(connection, expected_response="log")
                            yield log["log_record"]
                        except OperationTimeoutError:
                            yield None
                        # Successful log receive should reset retry counter
                        try_no = 0
            except (ConnectionError, TimeoutError, GatewayShutdownError):
                if self._connection:
                    self._connection.close()
                if try_no >= self.gateway_retries:
                    raise
            self._retry_delay(try_no)
            try_no += 1

    def increment_metrics(self, metric: KartonMetrics, identity: str) -> None:
        # This is no-op, Karton gateway manages all metrics
        return
