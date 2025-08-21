import contextlib
import json
import time
import urllib.parse
from io import BytesIO
from typing import IO, Any, Callable, Dict, Iterator, Optional, Tuple, Union

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
    KartonServiceType,
)


class GatewayError(Exception):
    code = ""

    def __init__(self, message: str, code: Optional[str] = None):
        self.code = code or self.code
        self.message = message


class NoTaskError(GatewayError):
    code = "no_task"


class GatewayBindExpiredError(GatewayError):
    code = "expired_bind"


def make_gateway_error(code: str, message: str) -> GatewayError:
    for error_class in GatewayError.__subclasses__():
        if error_class.code == code:
            return error_class(message)
    return GatewayError(code, message)


def override_presigned_url(presigned_url: str, override_host: str) -> Tuple[str, str]:
    """
    Overrides presigned URL with new hostname.
    Returns URL netloc (to be put in Host header) and new URL
    """
    parsed_url = urllib.parse.urlparse(presigned_url)
    url_host = parsed_url.netloc
    return url_host, parsed_url._replace(netloc=override_host).geturl()


def serialize_resources(
    payload: Dict[str, Any]
) -> Tuple[Dict[str, Any], Dict[str, LocalResourceBase]]:
    local_resources: Dict[str, LocalResourceBase] = {}

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
                    "to_upload": to_upload,
                }
            }
        return obj

    return recursive_map(serialize_resource, payload), local_resources


def deserialize_resources(
    payload: Dict[str, Any],
    resource_deserializer: Callable[[Dict[str, Any]], RemoteResource],
) -> Dict[str, Any]:
    def deserialize_resource(obj: Any) -> Any:
        if type(obj) is dict and obj.keys() == {"__karton_resource__"}:
            return resource_deserializer(obj["__karton_resource__"])
        return obj

    return recursive_map(deserialize_resource, payload)


class KartonGatewayBackend(KartonBackendProtocol):
    def __init__(self, config: Config, service_info: KartonServiceInfo):
        self.config = config
        self.service_info = service_info

        self._connection: Optional[ClientConnection] = None
        self._karton_bind: Optional[KartonBind] = None
        self._connection_used: bool = False

        self.gateway_url = self.config.get("gateway", "url")
        self.gateway_username = self.config.get("gateway", "username")
        self.gateway_password = self.config.get("gateway", "password")
        self.gateway_s3_hostname_override = self.config.get(
            "gateway", "s3_hostname_override"
        )

    def _recv(
        self, connection: ClientConnection, expected_response: str, timeout: int = 10
    ) -> Dict[str, Any]:
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
        return message.get("message", {})

    def _send(self, connection: ClientConnection, message: Dict[str, Any]) -> None:
        data = json.dumps(message)
        connection.send(data)

    def _bind(self, connection: ClientConnection) -> None:
        if self.service_info.service_type in [
            KartonServiceType.PRODUCER,
            KartonServiceType.LOG_CONSUMER,
        ]:
            self._send(
                connection,
                {
                    "request": "bind",
                    "message": {
                        "service_type": self.service_info.service_type.value,
                        "identity": self.service_info.identity,
                        "library_version": self.service_info.karton_version,
                        "service_version": self.service_info.service_version,
                    },
                },
            )
            self._recv(connection, expected_response="success")
        if (
            self.service_info.service_type is KartonServiceType.CONSUMER
            and self._karton_bind is not None
        ):
            self._send(
                connection,
                {
                    "request": "bind",
                    "message": {
                        "service_type": "consumer",
                        "identity": self.service_info.identity,
                        "library_version": self.service_info.karton_version,
                        "service_version": self.service_info.service_version,
                        "info": self._karton_bind.info,
                        "filters": self._karton_bind.filters,
                        "persistent": self._karton_bind.persistent,
                        "is_async": self._karton_bind.is_async,
                    },
                },
            )
            self._recv(connection, expected_response="success")

    def _init_connection(self, connection: ClientConnection, service_bind=True) -> None:
        hello_message = self._recv(connection, expected_response="hello")
        if hello_message["auth_required"]:
            self._send(
                connection,
                {
                    "request": "auth",
                    "message": {
                        "username": self.gateway_username,
                        "password": self.gateway_password,
                    },
                },
            )
            self._recv(connection, expected_response="success")
        if service_bind:
            self._bind(connection)

    @contextlib.contextmanager
    def _get_connection(self) -> Iterator[ClientConnection]:
        if self._connection_used:
            # There are corner cases where Karton tries to send command
            # while connection is used for handling another one.
            # One of them is self.log.info("Got signal, shutting down...")
            # that is called on CTRL-C during waiting in consume_routed_task
            # This causes ConcurrencyError. Let's initiate another temporary
            # connection without bind in that case.
            _connection = connect(self.gateway_url)
            self._init_connection(_connection, service_bind=False)
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
            self._connection = connect(self.gateway_url)
            self._init_connection(self._connection)
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

    def declare_task(self, task: Task) -> None:
        # Serialize resources
        payload, resources = serialize_resources(task.payload)
        payload_persistent, resources_persistent = serialize_resources(
            task.payload_persistent
        )
        resources.update(resources_persistent)
        print("payload", payload)
        with self._get_connection() as connection:
            self._send(
                connection,
                {
                    "request": "declare_task",
                    "message": {
                        "task": {
                            "headers": task.headers,
                            "headers_persistent": task.headers_persistent,
                            "payload": payload,
                            "payload_persistent": payload_persistent,
                            "priority": task.priority.value,
                        },
                        "token": task.parent_token,
                    },
                },
            )
            response = self._recv(connection, expected_response="task_declared")

        task.uid = response["uid"]
        task.root_uid = root_uid_from_task_uid(response["uid"])
        task.bind_token(response["token"])
        for upload_url in response["resources_to_upload"]:
            resources[upload_url["uid"]].bind_upload_url(upload_url["upload_url"])

    def set_task_status(self, task: Task, status: TaskState) -> None:
        if task.status == status:
            return
        task.status = status
        task.last_update = time.time()
        with self._get_connection() as connection:
            message: Dict[str, Any] = {"token": task.token, "status": status.value}
            if status is TaskState.CRASHED:
                message["error"] = task.error
            self._send(connection, {"request": "set_task_status", "message": message})
            self._recv(connection, expected_response="success")

    def produce_unrouted_task(self, task: Task) -> None:
        with self._get_connection() as connection:
            self._send(
                connection,
                {
                    "request": "send_task",
                    "message": {
                        "token": task.token,
                    },
                },
            )
            self._recv(connection, expected_response="success")

    def register_bind(self, bind: KartonBind) -> None:
        self._karton_bind = bind
        with self._get_connection() as connection:
            self._bind(connection)

    def consume_routed_task(
        self, identity: str, timeout: int = 5, _bind: Optional[KartonBind] = None
    ) -> Optional[Task]:
        with self._get_connection() as connection:
            self._send(connection, {"request": "get_task"})
            try:
                response = self._recv(connection, expected_response="task")
            except NoTaskError:
                return None
            except GatewayBindExpiredError as e:
                raise BindExpiredError("Binds changed, shutting down") from e

        download_urls = {
            spec["uid"]: spec["download_url"] for spec in response["resources"]
        }

        def deserialize_resource(resource_data: Dict[str, Any]) -> RemoteResource:
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

    def increment_metrics(self, metric: KartonMetrics, identity: str) -> None:
        # This is performed by Karton Gateway
        return

    def upload_object(
        self, resource: LocalResource, content: Union[bytes, IO[bytes]]
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

    def upload_object_from_file(self, resource: LocalResource, path: str) -> None:
        with open(path, "rb") as f:
            self.upload_object(resource, f)

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

    def download_object(self, resource: RemoteResource) -> bytes:
        with self._get_download_stream(resource) as response:
            response.raise_for_status()
            return response.read()

    def download_object_to_file(self, resource: RemoteResource, path: str) -> None:
        with self._get_download_stream(resource) as response:
            response.raise_for_status()
            with open(path, "wb") as f:
                for chunk in response.iter_bytes():
                    f.write(chunk)

    def produce_log(
        self, log_record: Dict[str, Any], logger_name: str, level: str
    ) -> bool:
        with self._get_connection() as connection:
            self._send(
                connection,
                {
                    "request": "send_log",
                    "message": {
                        "log_record": log_record,
                        "logger_name": logger_name,
                        "level": level,
                    },
                },
            )
            status = self._recv(connection, expected_response="log_sent")
            return status["was_received"]

    def remove_object(self, bucket: str, object_uid: str) -> None:
        # This method is called by RemoteResource.remove,
        # but we don't implement this function in gateway
        raise NotImplementedError

    def consume_log(
        self,
        timeout: int = 5,
        logger_filter: Optional[str] = None,
        level: Optional[str] = None,
    ) -> Iterator[Optional[Dict[str, Any]]]:
        raise NotImplementedError
