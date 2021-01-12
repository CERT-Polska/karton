import argparse
import logging
import os.path
from configparser import ConfigParser
from typing import Any, Dict, List

from minio import Minio
from redis import StrictRedis

from .__version__ import __version__
from .backend import KartonBackend
from .config import Config
from .karton import Consumer

log = logging.getLogger(__name__)


def get_user_option(prompt: str, default: str) -> str:
    user_input = input(f"{prompt}\n[{default}] ")
    print("")  # just for style
    return user_input.strip() or default


def configuration_wizard(config_filename: str) -> None:
    config = ConfigParser()

    log.info("Configuring MinIO")
    while True:
        minio_access_key = get_user_option(
            "Enter the MinIO access key", default="minioadmin"
        )
        minio_secret_key = get_user_option(
            "Enter the MinIO secret key", default="minioadmin"
        )
        minio_address = get_user_option(
            "Enter the MinIO address", default="localhost:9000"
        )
        minio_bucket = get_user_option(
            "Enter the MinIO bucket to use", default="karton"
        )
        minio_secure = get_user_option('Use SSL ("0", "1")?', default="0")

        log.info("Testing MinIO connection...")
        minio = Minio(
            endpoint=minio_address,
            access_key=minio_access_key,
            secret_key=minio_secret_key,
            secure=bool(int(minio_secure)),
        )
        bucket_exists = False
        try:
            bucket_exists = minio.bucket_exists(minio_bucket)
        except Exception as e:
            log.info("Error while connecting to MinIO: %s", e, exc_info=True)
            retry = get_user_option(
                'Do you want to try with different MinIO settings ("yes", "no")?',
                default="yes",
            )
            if retry != "yes":
                log.info("Quitting configuration")
                return
            else:
                continue

        log.info("Connected to MinIO successfully")
        if not bucket_exists:
            log.info(
                (
                    "The required bucket %s does not exist. To create it automatically,"
                    " start karton-system with --setup-bucket flag"
                ),
                minio_bucket,
            )
        break

    config["minio"] = {
        "access_key": minio_access_key,
        "secret_key": minio_secret_key,
        "address": minio_address,
        "bucket": minio_bucket,
        "secure": str(bool(int(minio_secure))),
    }

    log.info("Configuring Redis")

    while True:
        redis_host = get_user_option("Enter the Redis host", default="localhost")
        redis_port = get_user_option("Enter the Redis port", default="6379")

        log.info("Testing Redis connection...")
        redis = StrictRedis(
            host=redis_host,
            port=int(redis_port),
            decode_responses=True,
        )
        try:
            redis.ping()
        except Exception as e:
            log.info("Error while connecting to Redis: %s", e, exc_info=True)
            retry = get_user_option(
                'Do you want to try with different Redis settings ("yes", "no")?',
                default="yes",
            )
            if retry != "yes":
                log.info("Quitting configuration")
                return
            else:
                continue

        log.info("Connected to Redis successfully")
        break

    config["redis"] = {
        "host": redis_host,
        "port": str(int(redis_port)),
    }

    with open(config_filename, "w") as configfile:
        config.write(configfile)

    log.info("Saved the new configuration file in %s", os.path.abspath(config_filename))


def print_bind_list(config: Config) -> None:
    backend = KartonBackend(config=config)
    for bind in backend.get_binds():
        print(bind)


def delete_bind(config: Config, karton_name: str) -> None:
    backend = KartonBackend(config=config)
    binds = {k.identity: k for k in backend.get_binds()}
    consumers = backend.get_online_consumers()

    if karton_name not in binds:
        log.error("Trying to delete a karton bind that doesn't exist")
        return

    if consumers.get(karton_name, []):
        log.error(
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
    parser.add_argument("--version", action="version", version=__version__)
    parser.add_argument("-c", "--config-file", help="Alternative configuration path")
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="More verbose log output"
    )

    subparsers = parser.add_subparsers(dest="command", help="sub-command help")

    subparsers.add_parser("list", help="List active karton binds")

    delete_parser = subparsers.add_parser("delete", help="Delete an unused karton bind")
    delete_parser.add_argument("identity", help="Karton bind identity to remove")

    configure_parser = subparsers.add_parser(
        "configure", help="Create a new configuration file"
    )
    configure_parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        help="Overwrite the existing configuration file",
    )

    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    if args.command == "configure":
        config_filename = args.config_file or "./karton.ini"

        log.debug("Creating a new configuration file in %s", config_filename)

        if not args.force and os.path.exists(config_filename):
            log.error(
                (
                    "There's already a configuration file under %s. Please delete "
                    "it or specify a different filename using the -c argument"
                ),
                config_filename,
            )
            return

        configuration_wizard(config_filename)
        return

    try:
        config = Config(args.config_file)
    except RuntimeError as e:
        log.error("Error while initializing the karton config: %s", e)
        log.error(
            (
                "Please correct the configuration file or run `karton configure` "
                "to initialize it"
            )
        )
        return

    if args.command == "list":
        print_bind_list(config)
    elif args.command == "delete":
        karton_name = args.identity
        print(
            f"Are you sure you want to remove binds for karton {karton_name}?\n"
            "Type in the karton name to confirm deletion."
        )
        if input().strip() == karton_name:
            delete_bind(config, karton_name)
        else:
            log.info("Aborted.")
    else:
        parser.print_help()
