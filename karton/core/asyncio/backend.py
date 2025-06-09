import json
import logging
import time
from typing import IO, Any, Dict, List, Optional, Tuple, Union

import aioboto3
from aiobotocore.credentials import ContainerProvider, InstanceMetadataProvider
from aiobotocore.session import ClientCreatorContext, get_session
from aiobotocore.utils import InstanceMetadataFetcher
from redis.asyncio import Redis
from redis.asyncio.client import Pipeline
from redis.exceptions import AuthenticationError

from karton.core import Config, Task
from karton.core.asyncio.resource import RemoteResource
from karton.core.backend import (
    KARTON_BINDS_HSET,
    KARTON_TASK_NAMESPACE,
    KARTON_TASKS_QUEUE,
    KartonBackendBase,
    KartonBind,
    KartonMetrics,
    KartonServiceInfo,
)
from karton.core.task import TaskState

logger = logging.getLogger(__name__)


class KartonAsyncBackend(KartonBackendBase):
    def __init__(
        self,
        config: Config,
        identity: Optional[str] = None,
        service_info: Optional[KartonServiceInfo] = None,
    ) -> None:
        super().__init__(config, identity, service_info)
        self._redis: Optional[Redis] = None
        self._s3_session: Optional[aioboto3.Session] = None
        self._s3_iam_auth = False

    @property
    def redis(self) -> Redis:
        if not self._redis:
            raise RuntimeError("Call connect() first before using KartonAsyncBackend")
        return self._redis

    @property
    def s3(self) -> ClientCreatorContext:
        if not self._s3_session:
            raise RuntimeError("Call connect() first before using KartonAsyncBackend")
        endpoint = self.config.get("s3", "address")
        if self._s3_iam_auth:
            return self._s3_session.client(
                "s3",
                endpoint_url=endpoint,
            )
        else:
            access_key = self.config.get("s3", "access_key")
            secret_key = self.config.get("s3", "secret_key")
            return self._s3_session.client(
                "s3",
                endpoint_url=endpoint,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
            )

    async def connect(self):
        if self._redis is not None or self._s3_session is not None:
            # Already connected
            return
        self._redis = await self.make_redis(
            self.config, identity=self.identity, service_info=self.service_info
        )

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

    async def iam_auth_s3(self):
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
                boto_session._credentials = creds  # type: ignore
                return aioboto3.Session(botocore_session=boto_session)

    @staticmethod
    async def make_redis(
        config,
        identity: Optional[str] = None,
        service_info: Optional[KartonServiceInfo] = None,
    ) -> Redis:
        """
        Create and test a Redis connection.

        :param config: The karton configuration
        :param identity: Karton service identity
        :param service_info: Additional service identity metadata
        :return: Redis connection
        """
        if service_info is not None:
            client_name: Optional[str] = service_info.make_client_name()
        else:
            client_name = identity

        redis_args = {
            "host": config["redis"]["host"],
            "port": config.getint("redis", "port", 6379),
            "db": config.getint("redis", "db", 0),
            "username": config.get("redis", "username"),
            "password": config.get("redis", "password"),
            "client_name": client_name,
            # set socket_timeout to None if set to 0
            "socket_timeout": config.getint("redis", "socket_timeout", 30) or None,
            "decode_responses": True,
        }
        try:
            rs = Redis(**redis_args)
            await rs.ping()
        except AuthenticationError:
            # Maybe we've sent a wrong password.
            # Or maybe the server is not (yet) password protected
            # To make smooth transition possible, try to login insecurely
            del redis_args["password"]
            rs = Redis(**redis_args)
            await rs.ping()
        return rs

    def unserialize_resource(self, resource_spec: Dict[str, Any]) -> RemoteResource:
        """
        Unserializes resource into a RemoteResource object bound with current backend

        :param resource_spec: Resource specification
        :return: RemoteResource object
        """
        return RemoteResource.from_dict(resource_spec, backend=self)

    async def register_task(self, task: Task, pipe: Optional[Pipeline] = None) -> None:
        """
        Register or update task in Redis.

        :param task: Task object
        :param pipe: Optional pipeline object if operation is a part of pipeline
        """
        rs = pipe or self.redis
        await rs.set(f"{KARTON_TASK_NAMESPACE}:{task.uid}", task.serialize())

    async def set_task_status(
        self, task: Task, status: TaskState, pipe: Optional[Pipeline] = None
    ) -> None:
        """
        Request task status change to be applied by karton-system

        :param task: Task object
        :param status: New task status (TaskState)
        :param pipe: Optional pipeline object if operation is a part of pipeline
        """
        if task.status == status:
            return
        task.status = status
        task.last_update = time.time()
        await self.register_task(task, pipe=pipe)

    async def register_bind(self, bind: KartonBind) -> Optional[KartonBind]:
        """
        Register bind for Karton service and return the old one

        :param bind: KartonBind object with bind definition
        :return: Old KartonBind that was registered under this identity
        """
        async with self.redis.pipeline(transaction=True) as pipe:
            await pipe.hget(KARTON_BINDS_HSET, bind.identity)
            await pipe.hset(KARTON_BINDS_HSET, bind.identity, self.serialize_bind(bind))
            old_serialized_bind, _ = await pipe.execute()

        if old_serialized_bind:
            return self.unserialize_bind(bind.identity, old_serialized_bind)
        else:
            return None

    async def get_bind(self, identity: str) -> KartonBind:
        """
        Get bind object for given identity

        :param identity: Karton service identity
        :return: KartonBind object
        """
        return self.unserialize_bind(
            identity, await self.redis.hget(KARTON_BINDS_HSET, identity)
        )

    async def produce_unrouted_task(self, task: Task) -> None:
        """
        Add given task to unrouted task (``karton.tasks``) queue

        Task must be registered before with :py:meth:`register_task`

        :param task: Task object
        """
        await self.redis.rpush(KARTON_TASKS_QUEUE, task.uid)

    async def consume_queues(
        self, queues: Union[str, List[str]], timeout: int = 0
    ) -> Optional[Tuple[str, str]]:
        """
        Get item from queues (ordered from the most to the least prioritized)
        If there are no items, wait until one appear.

        :param queues: Redis queue name or list of names
        :param timeout: Waiting for item timeout (default: 0 = wait forever)
        :return: Tuple of [queue_name, item] objects or None if timeout has been reached
        """
        return await self.redis.blpop(queues, timeout=timeout)

    async def get_task(self, task_uid: str) -> Optional[Task]:
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
        self, identity: str, timeout: int = 5
    ) -> Optional[Task]:
        """
        Get routed task for given consumer identity.

        If there are no tasks, blocks until new one appears or timeout is reached.

        :param identity: Karton service identity
        :param timeout: Waiting for task timeout (default: 5)
        :return: Task object
        """
        item = await self.consume_queues(
            self.get_queue_names(identity),
            timeout=timeout,
        )
        if not item:
            return None
        queue, data = item
        return await self.get_task(data)

    async def increment_metrics(
        self, metric: KartonMetrics, identity: str, pipe: Optional[Pipeline] = None
    ) -> None:
        """
        Increments metrics for given operation type and identity

        :param metric: Operation metric type
        :param identity: Related Karton service identity
        :param pipe: Optional pipeline object if operation is a part of pipeline
        """
        rs = pipe or self.redis
        await rs.hincrby(metric.value, identity, 1)

    async def upload_object(
        self,
        bucket: str,
        object_uid: str,
        content: Union[bytes, IO[bytes]],
    ) -> None:
        """
        Upload resource object to underlying object storage (S3)

        :param bucket: Bucket name
        :param object_uid: Object identifier
        :param content: Object content as bytes or file-like stream
        """
        async with self.s3 as client:
            await client.put_object(Bucket=bucket, Key=object_uid, Body=content)

    async def upload_object_from_file(
        self, bucket: str, object_uid: str, path: str
    ) -> None:
        """
        Upload resource object file to underlying object storage

        :param bucket: Bucket name
        :param object_uid: Object identifier
        :param path: Path to the object content
        """
        async with self.s3 as client:
            with open(path, "rb") as f:
                await client.put_object(Bucket=bucket, Key=object_uid, Body=f)

    async def download_object(self, bucket: str, object_uid: str) -> bytes:
        """
        Download resource object from object storage.

        :param bucket: Bucket name
        :param object_uid: Object identifier
        :return: Content bytes
        """
        async with self.s3 as client:
            obj = await client.get_object(Bucket=bucket, Key=object_uid)
            return await obj["Body"].read()

    async def download_object_to_file(
        self, bucket: str, object_uid: str, path: str
    ) -> None:
        """
        Download resource object from object storage to file

        :param bucket: Bucket name
        :param object_uid: Object identifier
        :param path: Target file path
        """
        async with self.s3 as client:
            await client.download_file(Bucket=bucket, Key=object_uid, Filename=path)

    async def produce_log(
        self,
        log_record: Dict[str, Any],
        logger_name: str,
        level: str,
    ) -> bool:
        """
        Push new log record to the logs channel

        :param log_record: Dict with log record
        :param logger_name: Logger name
        :param level: Log level
        :return: True if any active log consumer received log record
        """
        return (
            await self.redis.publish(
                self._log_channel(logger_name, level), json.dumps(log_record)
            )
            > 0
        )
