import argparse
import json
from collections import namedtuple, Counter

from redis import StrictRedis

from .__version__ import __version__
from .utils import get_user_input
from .config import Config
from .karton import Consumer

KartonBind = namedtuple(
    "KartonBind", ["identity", "replicas", "persistent", "version", "filters"]
)


def get_service_binds(config):
    redis = StrictRedis(decode_responses=True, **config.redis_config)
    replica_no = Counter()

    # count replicas for each identity
    for client in redis.client_list():
        replica_no[client["name"]] += 1

    binds = redis.hgetall("karton.binds")
    services = []
    for identity, data in binds.items():
        val = json.loads(data)
        # karton 2.x compatibility :(
        if isinstance(val, list):
            val = val[0]

        services.append(
            KartonBind(
                identity,
                replica_no[identity],
                val.get("persistent", True),
                val.get("version", "2.x.x"),
                val.get("filters", []),
            )
        )
    return services


def print_bind_list(config):
    for k in get_service_binds(config):
        print(k)


def delete_bind(config, karton_name):
    binds = {k.identity: k for k in get_service_binds(config)}
    if karton_name not in binds:
        print("Trying to delete a karton bind that doesn't exist")
        return

    if binds[karton_name].replicas:
        print(
            "This bind has active replicas that need to be downscaled before it can be deleted"
        )
        return

    class KartonDummy(Consumer):
        persistent = False
        filters = []

        def process(self):
            pass

    karton = KartonDummy(config=config, identity=karton_name)
    karton.shutdown = True
    karton.loop()


def main():
    parser = argparse.ArgumentParser(description="Your red pill to the karton-verse")
    parser.add_argument("--list", action="store_true", help="List active karton binds")
    parser.add_argument(
        "--delete", action="store", help="Delete persistent karton bind"
    )
    parser.add_argument("--version", action="version", version=__version__)
    parser.add_argument("--config-file", help="Alternative configuration path")
    args = parser.parse_args()

    config = Config(args.config_file)

    if args.delete:
        karton_name = args.delete
        print(
            "Are you sure you want to remove binds for karton {karton_name}?\nType in the karton name to confirm".format(
                karton_name=karton_name
            )
        )
        if get_user_input().strip() == karton_name:
            delete_bind(config, karton_name)
        return

    if args.list:
        print_bind_list(config)
        return
