from collections import namedtuple
import json

from redis import StrictRedis
from minio import Minio


KARTON_BINDS_HSET = "karton.binds"

KartonBind = namedtuple(
    "KartonBind", ["identity", "info", "version", "persistent", "filters"]
)


class KartonBackend:
    def __init__(self, config):
        self.redis = StrictRedis(host=config["redis"]["host"],
                                 port=config["redis"]["port"],
                                 decode_responses=True)
        self.minio = Minio(
            config["minio"]["address"],
            access_key=config["minio"]["access_key"],
            secret_key=config["minio"]["secret_key"],
            secure=bool(int(config["minio"].get("secure", True))),
        )

    @staticmethod
    def serialize_bind(bind):
        """
        Serialize KartonBind object (Karton service registration)

        :param bind: KartonBind object with bind definition
        :return: Serialized bind data
        """
        return json.dumps(
            {
                "info": bind.info,
                "version": bind.version,
                "filters": bind.filters,
                "persistent": bind.persistent
            },
            sort_keys=True
        )

    @staticmethod
    def unserialize_bind(identity, bind_data):
        """
        Deserialize KartonBind object for given identity.
        Compatible with Karton 2.x.x and 3.x.x

        :param identity: Karton service identity
        :param bind_data: Serialized bind data
        :return: KartonBind object with bind definition
        """
        bind = json.loads(bind_data)
        if isinstance(bind, list):
            # Backwards compatibility (v2.x.x)
            return KartonBind(
                identity=identity,
                info=None,
                version="2.x.x",
                persistent=not identity.endswith(".test"),
                filters=bind
            )
        return KartonBind(
            identity=identity,
            info=bind["info"],
            version=bind["version"],
            persistent=bind["persistent"],
            filters=bind["filters"]
        )

    def get_bind(self, identity):
        """
        Get bind object for given identity

        :param identity: Karton service identity
        :return: KartonBind object
        """
        return self.unserialize_bind(identity, self.redis.hget(KARTON_BINDS_HSET, identity))

    def get_binds(self):
        """
        Get all binds registered in Redis

        :return: List of KartonBind objects for subsequent identities
        """
        return [
            self.unserialize_bind(identity, raw_bind)
            for identity, raw_bind in self.redis.hgetall(KARTON_BINDS_HSET).items()
        ]

    def register_bind(self, bind):
        """
        Register bind for Karton service and return the old one

        :param bind: KartonBind object with bind definition
        """
        with self.redis.pipeline(transaction=True) as pipe:
            pipe.hget(KARTON_BINDS_HSET, bind.identity)
            pipe.hset(KARTON_BINDS_HSET, bind.identity, self.serialize_bind(bind))
            old_serialized_bind, _ = pipe.execute()

        if old_serialized_bind:
            return self.unserialize_bind(bind.identity, old_serialized_bind)
        else:
            return None

    def remove_bind(self, identity):
        """
        Removes bind for identity
        """
        self.redis.hdel(KARTON_BINDS_HSET, identity)
