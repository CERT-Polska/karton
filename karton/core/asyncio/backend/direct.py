import json
import logging
import os
import time
from collections import defaultdict
from collections.abc import AsyncIterator, Iterable
from typing import IO, Any, cast

import aioboto3
from aiobotocore.credentials import ContainerProvider, InstanceMetadataProvider
from aiobotocore.session import ClientCreatorContext, get_session
from aiobotocore.utils import InstanceMetadataFetcher
from redis.asyncio import Redis
from redis.asyncio.client import Pipeline
from redis.exceptions import AuthenticationError

from karton.core import Config, Task
from karton.core.asyncio.resource import LocalResource, RemoteResource, ResourceData
from karton.core.backend import KartonBind, KartonMetrics, KartonServiceInfo
from karton.core.backend.base import KartonExternalServiceInfo, KartonOutputs
from karton.core.backend.direct import (
    KARTON_BINDS_HSET,
    KARTON_OUTPUTS_NAMESPACE,
    KARTON_SERVICES_NAMESPACE,
    KARTON_TASK_NAMESPACE,
    KARTON_TASKS_QUEUE,
    KartonBackendBase,
)
from karton.core.exceptions import BindExpiredError, HeartbeatExpiredError
from karton.core.resource import LocalResource as SyncLocalResource
from karton.core.task import TaskState
from karton.core.utils import achunks_iter

from .base import KartonAsyncBackendProtocol

logger = logging.getLogger(__name__)


class KartonAsyncBackend(KartonBackendBase, KartonAsyncBackendProtocol):
    def __init__(
        self,
        config: Config,
        service_info: KartonServiceInfo,
        _redis: Redis | None = None,
        _s3_session: aioboto3.Session | None = None,
        _s3_iam_auth=False,
    ) -> None:
        super().__init__(config, service_info)
        self._redis: Redis | None = _redis
        self._s3_session: aioboto3.Session | None = _s3_session
        self._s3_iam_auth = _s3_iam_auth

    @property
    def redis(self) -> Redis:
        if not self._redis:
            raise RuntimeError("Call connect() first before using KartonAsyncBackend")
        return self._redis

    @property
    def s3(self) -> ClientCreatorContext:
        if not self._s3_session:
            raise RuntimeError("Call connect() first before using KartonAsyncBackend")
        endpoint = self.config.get("s3", "address") or os.getenv("AWS_ENDPOINT_URL")
        if self._s3_iam_auth:
            return self._s3_session.client(
                "s3",
                endpoint_url=endpoint,
            )
        else:
            access_key = self.config.get("s3", "access_key") or os.getenv(
                "AWS_ACCESS_KEY_ID"
            )
            secret_key = self.config.get("s3", "secret_key") or os.getenv(
                "AWS_SECRET_ACCESS_KEY"
            )
            return self._s3_session.client(
                "s3",
                endpoint_url=endpoint,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
            )

    async def connect(self) -> None:
        if self._redis is not None or self._s3_session is not None:
            # Already connected
            return
        self._redis = await self.make_redis(self.config, service_info=self.service_info)

        endpoint = self.config.get("s3", "address")
        access_key = self.config.get("s3", "access_key")
        secret_key = self.config.get("s3", "secret_key")
        iam_auth = self.config.getboolean("s3", "iam_auth")

        if not endpoint:
            raise RuntimeError("Attempting to get S3 client without an endpoint set")

        if access_key and secret_key and iam_auth:
            logger.warning(
                "Warning: iam is turned on and both S3 access key and secret key are"
                " provided"
            )

        if iam_auth:
            s3_client_creator = await self.iam_auth_s3()
            if s3_client_creator:
                self._s3_iam_auth = True
                self._s3_session = s3_client_creator
                return

        if access_key is None or secret_key is None:
            raise RuntimeError(
                "Attempting to get S3 client without an access_key/secret_key set"
            )

        session = aioboto3.Session()
        self._s3_session = session

    async def close(self) -> None:
        if self._redis is not None:
            await self._redis.close()
            self._redis = None
        self._s3_session = None
        self._s3_iam_auth = None

    async def iam_auth_s3(self) -> aioboto3.Session | None:
        boto_session = get_session()
        iam_providers = [
            ContainerProvider(),
            InstanceMetadataProvider(
                iam_role_fetcher=InstanceMetadataFetcher(timeout=1000, num_attempts=2)
            ),
        ]

        for provider in iam_providers:
            creds = await provider.load()
            if creds:
                boto_session._credentials = creds
                return aioboto3.Session(botocore_session=boto_session)
        return None

    @classmethod
    async def make_redis(
        cls,
        config,
        service_info: KartonServiceInfo,
    ) -> Redis:
        """
        Create and test a Redis connection.

        :param config: The karton configuration
        :param service_info: Service identity metadata
        :return: Redis connection
        """
        redis_args = cls.get_redis_configuration(config, service_info)
        try:
            if "url" in redis_args:
                rs = Redis.from_url(**redis_args)
            else:
                rs = Redis(**redis_args)
            await rs.ping()
        except AuthenticationError:
            # Maybe we've sent a wrong password.
            # Or maybe the server is not (yet) password protected
            # To make smooth transition possible, try to login insecurely
            if "username" in redis_args:
                del redis_args["username"]
            if "password" in redis_args:
                del redis_args["password"]
            if "url" in redis_args:
                rs = Redis.from_url(**redis_args)
            else:
                rs = Redis(**redis_args)
            await rs.ping()
        return rs

    def unserialize_resource(self, resource_spec: ResourceData) -> RemoteResource:
        """
        Unserializes resource into a RemoteResource object bound with current backend

        :param resource_spec: Resource specification
        :return: RemoteResource object
        """
        return RemoteResource.from_dict(resource_spec, backend=self)

    async def _iter_tasks(
        self,
        task_keys: AsyncIterator[str],
        chunk_size: int = 1000,
        parse_resources: bool = True,
    ) -> AsyncIterator[Task]:
        async for chunk in achunks_iter(task_keys, chunk_size):
            for task_data in await self.redis.mget(chunk):
                if task_data is None:
                    continue
                yield Task.unserialize(
                    task_data,
                    resource_unserializer=self.unserialize_resource
                    if parse_resources
                    else None,
                )

    def iter_tasks(
        self,
        task_uid_list: Iterable[str],
        chunk_size: int = 1000,
        parse_resources: bool = True,
    ) -> AsyncIterator[Task]:
        """
        Get multiple tasks for given identifier list as an iterator

        :param task_uid_list: List of task fully-qualified identifiers
        :param chunk_size: Size of chunks passed to the Redis MGET command
        :param parse_resources: If set to False, resources are not parsed.
            It speeds up deserialization. Read :py:meth:`Task.unserialize` documentation
            to learn more.
        :return: Iterator with task objects
        """

        async def _key_iter(uid_list: Iterable[str]) -> AsyncIterator[str]:
            for uid in uid_list:
                yield f"{KARTON_TASK_NAMESPACE}:{uid}"

        return self._iter_tasks(
            _key_iter(task_uid_list),
            chunk_size=chunk_size,
            parse_resources=parse_resources,
        )

    def iter_all_tasks(
        self, chunk_size: int = 1000, parse_resources: bool = True
    ) -> AsyncIterator[Task]:
        """
        Iterates all tasks registered in Redis
        :param chunk_size: Size of chunks passed to the Redis SCAN and MGET command
        :param parse_resources: If set to False, resources are not parsed.
            It speeds up deserialization. Read :py:meth:`Task.unserialize` documentation
            to learn more.
        :return: Iterator with Task objects
        """
        task_keys = self.redis.scan_iter(
            match=f"{KARTON_TASK_NAMESPACE}:*", count=chunk_size
        )
        return self._iter_tasks(
            task_keys, chunk_size=chunk_size, parse_resources=parse_resources
        )

    async def _iter_legacy_task_tree(
        self, root_uid: str, chunk_size: int = 1000, parse_resources: bool = True
    ) -> AsyncIterator[Task]:
        """
        Processes tasks made by <5.4.0 (unrouted from <5.4.0 producers or existing
        before upgrade)

        Internal function for use by iter_task_tree.
        """
        # Iterate over all karton tasks that do not match the new task id format
        legacy_task_keys = self.redis.scan_iter(
            match=f"{KARTON_TASK_NAMESPACE}:[^{{]*", count=chunk_size
        )
        async for chunk in achunks_iter(legacy_task_keys, chunk_size):
            for task_data in await self.redis.mget(chunk):
                if task_data is None:
                    continue
                task = Task.unserialize(
                    task_data,
                    resource_unserializer=self.unserialize_resource
                    if parse_resources
                    else None,
                )
                if task.root_uid == root_uid:
                    yield task

    async def iter_task_tree(
        self, root_uid: str, chunk_size: int = 1000, parse_resources: bool = True
    ) -> AsyncIterator[Task]:
        """
        Iterates all tasks that belong to the same analysis task tree
        and have the same root_uid

        :param root_uid: Root identifier of task tree
        :param chunk_size: Size of chunks passed to the Redis SCAN and MGET command
        :param parse_resources: If set to False, resources are not parsed.
            It speeds up deserialization. Read :py:meth:`Task.unserialize` documentation
            to learn more.
        :return: Iterator with task objects
        """
        # Process <5.4.0 tasks (unrouted from <5.4.0 producers
        # or existing before upgrade)
        async for task in self._iter_legacy_task_tree(
            root_uid, chunk_size=chunk_size, parse_resources=parse_resources
        ):
            yield task
        # Process >=5.4.0 tasks
        task_keys = self.redis.scan_iter(
            match=f"{KARTON_TASK_NAMESPACE}:{{{root_uid}}}:*", count=chunk_size
        )
        async for task in self._iter_tasks(
            task_keys, chunk_size=chunk_size, parse_resources=parse_resources
        ):
            yield task

    async def declare_task(self, task: Task) -> None:
        # Ensure all local resources have good buckets
        for resource in task.iterate_resources():
            if isinstance(resource, LocalResource) and not resource.bucket:
                resource.bucket = self.default_bucket_name
            if isinstance(resource, SyncLocalResource):
                raise RuntimeError(
                    "Synchronous resources are not supported. "
                    "Use karton.core.asyncio.resource module instead."
                )

        # Register new task
        await self.register_task(task)

    async def register_task(self, task: Task, pipe: Pipeline | None = None) -> None:
        """
        Register or update task in Redis.

        :param task: Task object
        :param pipe: Optional pipeline object if operation is a part of pipeline
        """
        rs = pipe or self.redis
        await rs.set(f"{KARTON_TASK_NAMESPACE}:{task.uid}", task.serialize())

    async def set_task_status(
        self, task: Task, status: TaskState, pipe: Pipeline | None = None
    ) -> None:
        """
        Extends :meth:`KartonAsyncBackendProtocol.set_task_status` with a
        Direct-backend-only ``pipe`` argument.

        :param pipe: Optional pipeline object if operation is a part of pipeline
        """
        task.status = status
        task.last_update = time.time()
        await self.register_task(task, pipe=pipe)

    async def register_bind(
        self, bind: KartonBind, bind_backend: bool = True
    ) -> KartonBind | None:
        """
        Extends :meth:`KartonAsyncBackendProtocol.register_bind` with a
        Direct-backend-only ``bind_backend`` argument.

        :param bind_backend: |
            Store the KartonBind in the backend object for consume_routed_task
            comparison to validate whether bind is still valid for current consumer.
            This flag is set to False by Karton Gateway because it reuses backend for
            multiple independent Karton services.
        """
        async with self.redis.pipeline(transaction=True) as pipe:
            await pipe.hget(KARTON_BINDS_HSET, bind.identity)
            await pipe.hset(KARTON_BINDS_HSET, bind.identity, self.serialize_bind(bind))
            old_serialized_bind, _ = await pipe.execute()

        if bind_backend:
            self._current_bind = bind
        if old_serialized_bind:
            return self.unserialize_bind(bind.identity, old_serialized_bind)
        else:
            return None

    async def _get_online_gateway_services(
        self,
    ) -> AsyncIterator[KartonExternalServiceInfo]:
        async for service_keys in achunks_iter(
            self.redis.scan_iter(
                match=f"{KARTON_SERVICES_NAMESPACE}:*",
                count=100,
            ),
            size=100,
        ):
            # KartonServiceInfo is returned per connection
            # so returned services can be duplicated
            for client_name in cast(
                list[str | None], await self.redis.mget(*service_keys)
            ):
                if not client_name:
                    continue
                try:
                    yield KartonExternalServiceInfo.parse_client_name(client_name)
                except Exception:
                    logger.exception(
                        "Fatal error while parsing client name: %s", client_name
                    )
                    continue

    async def _get_online_direct_services(
        self,
    ) -> AsyncIterator[KartonExternalServiceInfo]:
        for client in await self.redis.client_list():
            name = client["name"]
            try:
                service_info = KartonExternalServiceInfo.parse_client_name(
                    name, redis_client_info=client
                )
                yield service_info
            except Exception:
                logger.exception("Fatal error while parsing client name: %s", name)
                continue

    async def _get_online_services(self) -> AsyncIterator[KartonExternalServiceInfo]:
        # Services having instance_id should be returned only once per instance_id
        # Services without instance_id are counted by amount of Redis connections
        # so they should be returned as many times as they appear in the output
        services: defaultdict[KartonExternalServiceInfo, int] = defaultdict(int)
        async for service in self._get_online_direct_services():
            services[service] += 1
        async for service in self._get_online_gateway_services():
            services[service] += 1

        for service in services:
            if service.instance_id is None:
                for _ in range(services[service]):
                    yield service
            else:
                yield service

    async def get_online_services(self) -> list[KartonExternalServiceInfo]:
        """
        Gets all online services providing extended service information.

        .. versionadded:: 5.1.0

        :return: List of KartonExternalServiceInfo objects
        """
        return [el async for el in self._get_online_services()]

    async def get_online_identities(self) -> dict[str, list[KartonExternalServiceInfo]]:
        """
        Returns a dictionary for all online identities with list of
        KartonExternalServiceInfo of each instance

        :return: Dictionary {identity: [list of KartonExternalServiceInfo objects]}
        """
        identities: defaultdict[str, list[KartonExternalServiceInfo]] = defaultdict(
            list
        )
        async for service in self._get_online_services():
            identities[service.identity].append(service)
        return dict(identities)

    async def get_bind(self, identity: str) -> KartonBind | None:
        """
        Get bind object for given identity

        :param identity: Karton service identity
        :return: KartonBind object or None if not found
        """
        bind_data = cast(str | None, await self.redis.hget(KARTON_BINDS_HSET, identity))
        if not bind_data:
            return None
        return self.unserialize_bind(identity, bind_data)

    async def get_binds(self) -> list[KartonBind]:
        """
        Get all binds registered in Redis

        :return: List of KartonBind objects for subsequent identities
        """
        return [
            self.unserialize_bind(identity, raw_bind)
            for identity, raw_bind in cast(
                dict[str, str], await self.redis.hgetall(KARTON_BINDS_HSET)
            ).items()
        ]

    async def produce_unrouted_task(self, task: Task) -> None:
        await self.redis.rpush(KARTON_TASKS_QUEUE, task.uid)

    async def produce_routed_task(
        self, identity: str, task: Task, pipe: Pipeline | None = None
    ) -> None:
        """
        Add given task to routed task queue of given identity

        Task must be registered using :py:meth:`register_task`

        :param identity: Karton service identity
        :param task: Task object
        :param pipe: Optional pipeline object if operation is a part of pipeline
        """
        rs = pipe or self.redis
        await rs.rpush(self.get_queue_name(identity, task.priority), task.uid)

    async def consume_queues(
        self, queues: str | list[str], timeout: int = 0
    ) -> tuple[str, str] | None:
        """
        Get item from queues (ordered from the most to the least prioritized)
        If there are no items, wait until one appear.

        :param queues: Redis queue name or list of names
        :param timeout: Waiting for item timeout (default: 0 = wait forever)
        :return: Tuple of [queue_name, item] objects or None if timeout has been reached
        """
        return cast(
            tuple[str, str] | None, await self.redis.blpop(queues, timeout=timeout)
        )

    async def get_task(self, task_uid: str) -> Task | None:
        """
        Get task object with given identifier

        :param task_uid: Task identifier
        :return: Task object
        """
        task_data = await self.redis.get(f"{KARTON_TASK_NAMESPACE}:{task_uid}")
        if not task_data:
            return None
        return Task.unserialize(
            task_data, resource_unserializer=self.unserialize_resource
        )

    async def consume_routed_task(
        self, identity: str, timeout: int = 5, bind_id: str | None = None
    ) -> Task | None:
        """
        Extends :meth:`KartonAsyncBackendProtocol.consume_routed_task` with a
        Direct-backend-only ``bind_id`` argument.

        :param bind_id: |
            Bind identifier to be compared instead of the bind
            stored in backend. Internal flag for use by Karton Gateway.
        """
        if bind_id is None and self._current_bind is None:
            raise RuntimeError("Bug: Tried to consume task without registering bind")

        current_bind = await self.get_bind(identity)
        if bind_id is not None:
            current_bind_id = None
            if current_bind is not None:
                current_bind_id = self.compute_bind_id(current_bind)
            if current_bind_id != bind_id:
                raise BindExpiredError(
                    f"Binds changed, shutting down. New binds: {current_bind}"
                )
        elif current_bind != self._current_bind:
            raise BindExpiredError(
                "Binds changed, shutting down. "
                f"Old binds: {self._current_bind} "
                f"New binds: {current_bind}"
            )
        item = await self.consume_queues(
            self.get_queue_names(identity),
            timeout=timeout,
        )
        if not item:
            return None
        _queue, data = item
        return await self.get_task(data)

    async def restart_task(self, task: Task) -> Task:
        """
        Requeues consumed task back to the consumer queue.

        New task is created with new uid and can be consumed by any active replica.

        Original task is marked as finished.

        :param task: Task to be restarted
        :return: Restarted task object
        """
        new_task = task.fork_task()
        # Preserve orig_uid to point at unrouted task
        new_task.orig_uid = task.orig_uid
        new_task.status = TaskState.SPAWNED

        async with self.redis.pipeline(transaction=True) as pipe:
            await self.register_task(new_task, pipe=pipe)
            await self.produce_routed_task(
                new_task.headers["receiver"], new_task, pipe=pipe
            )
            await self.set_task_status(task, status=TaskState.FINISHED, pipe=pipe)
            await pipe.execute()
        return new_task

    async def increment_metrics(
        self, metric: KartonMetrics, identity: str, pipe: Pipeline | None = None
    ) -> None:
        """
        Extends :meth:`KartonAsyncBackendProtocol.increment_metrics` with a
        Direct-backend-only ``pipe`` argument.

        :param pipe: Optional pipeline object if operation is a part of pipeline
        """
        rs = pipe or self.redis
        await rs.hincrby(metric.value, identity, 1)

    async def get_metrics(self, metric: KartonMetrics) -> dict[str, int]:
        """
        Get a {karton-identity: current-number-of-tasks} mapping for a given metric.

        :param metric: Operation metric type
        """
        return {
            k: int(v)
            for k, v in cast(
                dict[str, str], await self.redis.hgetall(metric.value)
            ).items()
        }

    async def get_all_metrics(self) -> dict[KartonMetrics, dict[str, int]]:
        return {metric: await self.get_metrics(metric) for metric in KartonMetrics}

    async def upload_resource(
        self,
        resource: LocalResource,
        content: bytes | IO[bytes],
    ) -> None:
        if resource.bucket is None:
            raise RuntimeError(
                "Resource object can't be uploaded because its bucket is not set"
            )
        async with self.s3 as client:
            await client.put_object(
                Bucket=resource.bucket, Key=resource.uid, Body=content
            )

    async def upload_resource_from_file(
        self, resource: LocalResource, path: str
    ) -> None:
        if resource.bucket is None:
            raise RuntimeError(
                "Resource object can't be uploaded because its bucket is not set"
            )
        with open(path, "rb") as f:
            await self.upload_resource(resource, f)

    async def download_resource(self, resource: RemoteResource) -> bytes:
        if resource.bucket is None:
            raise RuntimeError(
                "Resource object can't be downloaded because its bucket is not set"
            )
        async with self.s3 as client:
            obj = await client.get_object(Bucket=resource.bucket, Key=resource.uid)
            return await obj["Body"].read()

    async def download_resource_to_file(
        self, resource: RemoteResource, path: str
    ) -> None:
        if resource.bucket is None:
            raise RuntimeError(
                "Resource object can't be downloaded because its bucket is not set"
            )
        async with self.s3 as client:
            await client.download_file(
                Bucket=resource.bucket, Key=resource.uid, Filename=path
            )

    async def produce_log(
        self,
        log_record: dict[str, Any],
        logger_name: str,
        level: str,
    ) -> bool:
        return (
            await self.redis.publish(
                self._log_channel(logger_name, level), json.dumps(log_record)
            )
            > 0
        )

    async def consume_log(
        self,
        timeout: int = 5,
        logger_filter: str | None = None,
        level: str | None = None,
    ) -> AsyncIterator[dict[str, Any] | None]:
        async with self.redis.pubsub() as pubsub:
            await pubsub.psubscribe(self._log_channel(logger_filter, level))
            while pubsub.subscribed:
                item = await pubsub.get_message(
                    ignore_subscribe_messages=True, timeout=timeout
                )
                if item and item["type"] == "pmessage":
                    body = json.loads(item["data"])
                    if "task" in body and isinstance(body["task"], str):
                        body["task"] = json.loads(body["task"])
                    yield body
                else:
                    # return control back to the caller in case a shutdown or some
                    # other action was requested and needs to be handled
                    yield None

    async def get_outputs(self) -> list[KartonOutputs]:
        """
        Get a list of the output types for each karton.

        :return: List of KartonOutputs
        """

        output_keys = cast(
            list[str], await self.redis.keys(f"{KARTON_OUTPUTS_NAMESPACE}:*")
        )
        return [
            self.unserialize_output(
                identity.split(":")[1],
                cast(set[str], await self.redis.smembers(identity)),
            )
            for identity in output_keys
        ]

    @staticmethod
    def redis_heartbeat_key(service_info: KartonServiceInfo, connection_id: str) -> str:
        return (
            f"{KARTON_SERVICES_NAMESPACE}:{service_info.identity}:"
            f"{service_info.instance_id}:{connection_id}"
        )

    async def register_service(
        self, service_info: KartonServiceInfo, connection_id: str, expires_after: int
    ) -> None:
        """
        Registers a connection for an online service.

        Karton Gateway multiplexes multiple services over single pool of Redis
        connections, so we can't use CLIENT INFO for tracking whether services
        are alive. Karton Gateway uses heartbeat-based approach to track them
        and this function registers a heartbeat-tracking key in Redis.

        Internal method for use by Karton Gateway.

        :param service_info: Service info of the connected service
        :param connection_id: Connection identifier
        :param expires_after: Time to live for the record (in seconds)
        """
        if service_info.instance_id is None:
            raise ValueError("instance_id in service_info can't be None")
        await self.redis.set(
            self.redis_heartbeat_key(service_info, connection_id),
            service_info.make_client_name(),
            ex=expires_after,
        )

    async def heartbeat_service(
        self, service_info: KartonServiceInfo, connection_id: str, expires_after: int
    ) -> None:
        """
        Extends a heartbeat for the registered connection of an online service

        See also: register_service

        Internal method for use by Karton Gateway.

        :param service_info: Service info of the connected service
        :param connection_id: Connection identifier
        :param expires_after: Time to live for the record (in seconds)
        """
        if service_info.instance_id is None:
            raise ValueError("instance_id in service_info can't be None")
        success = await self.redis.expire(
            self.redis_heartbeat_key(service_info, connection_id),
            expires_after,
        )
        if not success:
            raise HeartbeatExpiredError(
                "Heartbeat prematurely expired because it wasn't refreshed on time."
            )

    async def unregister_service(
        self, service_info: KartonServiceInfo, connection_id: str
    ) -> None:
        """
        Removes a record for a connection of an online service.
        If all connections are dropped, service is considered offline.

        See also: register_service

        Internal method for use by Karton Gateway.

        :param service_info: Service info of the connected service
        :param connection_id: Connection identifier
        """
        if service_info.instance_id is None:
            raise ValueError("instance_id in service_info can't be None")
        await self.redis.delete(
            self.redis_heartbeat_key(service_info, connection_id),
        )

    async def get_presigned_object_download_url(
        self, bucket: str, object_uid: str, expires_in: int = 3600
    ) -> str:
        """
        Creates presigned url for downloading resource (GET) from S3 bucket.

        :param bucket: Bucket name
        :param object_uid: Resource object identifier
        :param expires_in: URL expiration time in seconds
        :return: Presigned download URL
        """
        async with self.s3 as client:
            return await client.generate_presigned_url(
                "get_object",
                Params={
                    "Bucket": bucket,
                    "Key": object_uid,
                },
                ExpiresIn=expires_in,
            )

    async def get_presigned_object_upload_url(
        self, bucket: str, object_uid: str, expires_in: int = 3600
    ) -> str:
        """
        Creates presigned url for uploading resource (PUT) into S3 bucket.

        :param bucket: Bucket name
        :param object_uid: Resource object identifier
        :param expires_in: URL expiration time in seconds
        :return: Presigned upload URL
        """
        async with self.s3 as client:
            return await client.generate_presigned_url(
                "put_object",
                Params={
                    "Bucket": bucket,
                    "Key": object_uid,
                },
                ExpiresIn=expires_in,
            )
