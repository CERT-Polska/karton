import argparse
from typing import Any, Dict, List

from .__version__ import __version__
from .backend import KartonBackend
from .config import Config
from .karton import Consumer


def print_bind_list(config: Config) -> None:
    backend = KartonBackend(config=config)
    for bind in backend.get_binds():
        print(bind)


def delete_bind(config: Config, karton_name: str) -> None:
    backend = KartonBackend(config=config)
    binds = {k.identity: k for k in backend.get_binds()}
    consumers = backend.get_online_consumers()

    if karton_name not in binds:
        print("Trying to delete a karton bind that doesn't exist")
        return

    if consumers.get(karton_name, []):
        print(
            "This bind has active replicas that need to be downscaled "
            "before it can be deleted"
        )
        return

    class KartonDummy(Consumer):
        persistent = False
        filters: List[Dict[str, Any]] = []

        def process(self, task):
            pass

    karton = KartonDummy(config=config, identity=karton_name)
    karton.shutdown = True
    karton.loop()


def main() -> None:
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
            "Are you sure you want to remove binds for karton {karton_name}?\n"
            "Type in the karton name to confirm".format(karton_name=karton_name)
        )
        if input().strip() == karton_name:
            delete_bind(config, karton_name)
        else:
            print("abort")

        return

    if args.list:
        print_bind_list(config)
        return
