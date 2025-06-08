import logging
from typing import IO, Optional, Union

import aioboto3
from aiobotocore.credentials import ContainerProvider, InstanceMetadataProvider
from aiobotocore.session import ClientCreatorContext, get_session
from aiobotocore.utils import InstanceMetadataFetcher
from redis.asyncio import Redis
from redis.asyncio.client import Pipeline
from redis.exceptions import AuthenticationError

from karton.core import Config, Task
from karton.core.backend import (
    KARTON_TASK_NAMESPACE,
    KARTON_TASKS_QUEUE,
    KartonBackendBase,
    KartonMetrics,
    KartonServiceInfo,
)

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
        self._s3: Optional[ClientCreatorContext] = None

    @property
    def redis(self) -> Redis:
        if not self._redis:
            raise RuntimeError("Call connect() first before using KartonAsyncBackend")
        return self._redis

    @property
    def s3(self) -> ClientCreatorContext:
        if not self._s3:
            raise RuntimeError("Call connect() first before using KartonAsyncBackend")
        return self._s3

    async def connect(self):
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
            s3_client_creator = await self.iam_auth_s3(endpoint)
            if s3_client_creator:
                self._s3 = s3_client_creator
                return

        if access_key is None or secret_key is None:
            raise RuntimeError(
                "Attempting to get S3 client without an access_key/secret_key set"
            )

        session = aioboto3.Session()
        self._s3 = session.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )

    async def iam_auth_s3(self, endpoint: str):
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
                return aioboto3.Session(botocore_session=boto_session).client(
                    "s3",
                    endpoint_url=endpoint,
                )

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

    async def register_task(self, task: Task, pipe: Optional[Pipeline] = None) -> None:
        """
        Register or update task in Redis.

        :param task: Task object
        :param pipe: Optional pipeline object if operation is a part of pipeline
        """
        rs = pipe or self.redis
        await rs.set(f"{KARTON_TASK_NAMESPACE}:{task.uid}", task.serialize())

    async def produce_unrouted_task(self, task: Task) -> None:
        """
        Add given task to unrouted task (``karton.tasks``) queue

        Task must be registered before with :py:meth:`register_task`

        :param task: Task object
        """
        await self.redis.rpush(KARTON_TASKS_QUEUE, task.uid)

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
