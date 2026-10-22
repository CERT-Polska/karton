import argparse
import getpass
import importlib
import inspect
import json
import socket
import sys
from pathlib import Path
from typing import Any

import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec
from pydantic import ValidationError

from karton.core.asyncio.base import KartonAsyncBase
from karton.core.auth.models import (
    AllowedOperation,
    AuthClaims,
    ConsumeLogOperation,
    ConsumeTaskOperation,
    EnableDebugOperation,
    ProduceTaskOperation,
    RegisterBindOperation,
)
from karton.core.auth.signer import generate_new_keypair, issue_auth_token
from karton.core.auth.verifier import decode_auth_token
from karton.core.base import KartonBase

DEFAULT_KEYRING_DIR = Path.home() / ".karton"
DEFAULT_PRIVATE_KEY_PATH = DEFAULT_KEYRING_DIR / "private.pem"
DEFAULT_JWKS_PATH = Path("jwks.json")

# Service archetypes accepted by the --karton-type claims argument
KARTON_TYPE_CHOICES = ("producer", "consumer", "karton", "logconsumer")


def default_issuer() -> str:
    return f"{getpass.getuser()}@{socket.gethostname()}"


def get_claims_from_karton_class(
    karton_class: type[KartonBase] | type[KartonAsyncBase],
) -> AuthClaims:
    from karton.core import Consumer, LogConsumer, Producer
    from karton.core.asyncio import Consumer as AsyncConsumer
    from karton.core.asyncio import Producer as AsyncProducer

    claims = AuthClaims(identity=karton_class.identity, allowed_operations=[])

    if issubclass(karton_class, (Producer, AsyncProducer)):
        claims.allowed_operations.append(ProduceTaskOperation())

    if issubclass(karton_class, (Consumer, AsyncConsumer)):
        claims.allowed_operations.append(ConsumeTaskOperation())
        claims.allowed_operations.append(
            RegisterBindOperation(
                filters=karton_class.filters,
                persistent=karton_class.persistent,
            )
        )

    if issubclass(karton_class, LogConsumer):
        claims.allowed_operations.append(ConsumeLogOperation())
    return claims


def import_karton_class(dotted: str) -> type[KartonBase] | type[KartonAsyncBase]:
    # Make modules in the current directory importable (console-script entry
    # points don't add CWD to sys.path, unlike `python -m ...`).
    if "" not in sys.path:
        sys.path.insert(0, "")
    if ":" in dotted:
        module_name, _, class_name = dotted.partition(":")
        if not module_name or not class_name:
            raise ValueError(
                "Expected a 'module:ClassName' specifier, got: " + repr(dotted)
            )
        module = importlib.import_module(module_name)
        karton_class = getattr(module, class_name)
        if not isinstance(karton_class, type) or not issubclass(
            karton_class, (KartonBase, KartonAsyncBase)
        ):
            raise TypeError(f"{dotted} is not a Karton (sub)class")
        return karton_class

    # No class specified: auto-detect a single Karton subclass in the module.
    # Includes classes re-exported via the package's __init__, excluding the
    # framework's own base classes (anything under karton.core).
    module = importlib.import_module(dotted)
    framework_prefix = KartonBase.__module__.rsplit(".", 1)[0]
    karton_classes: list[type[KartonBase | KartonAsyncBase]] = []
    seen: set[int] = set()
    for _name, obj in inspect.getmembers(module, inspect.isclass):
        if id(obj) in seen:
            continue
        if not issubclass(obj, (KartonBase, KartonAsyncBase)):
            continue
        if obj in (KartonBase, KartonAsyncBase):
            continue
        if obj.__module__.startswith(framework_prefix):
            continue
        seen.add(id(obj))
        karton_classes.append(obj)
    if len(karton_classes) == 0:
        raise ValueError(f"No Karton (sub)class found in module {dotted!r}")
    if len(karton_classes) > 1:
        names = ", ".join(c.__name__ for c in karton_classes)
        raise ValueError(
            f"Multiple Karton classes found in module {dotted!r}: {names}. "
            "Specify one using 'module:ClassName'"
        )
    return karton_classes[0]


def parse_filters_args(raw_filters: list[str] | None) -> list[dict[str, Any]]:
    """
    Parse repeatable --filters arguments into a list of filter variants.

    Each argument must be a JSON list of objects, e.g.:
    --filters '[{"type": "example.RawFeed", "platform": ["linux"]}]'

    :param raw_filters: List of raw JSON strings provided via --filters
    :return: List of filter variant dictionaries
    """
    filters: list[dict[str, Any]] = []
    for raw in raw_filters or []:
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as e:
            raise ValueError(f"--filters value is not valid JSON: {raw!r} ({e})") from e
        if not isinstance(parsed, list) or not all(
            isinstance(variant, dict) for variant in parsed
        ):
            raise ValueError(f"--filters expects a JSON list of objects, got: {raw!r}")
        filters.extend(parsed)
    return filters


def make_register_bind_operations(
    filters: list[dict[str, Any]],
    persistent: bool,
    non_persistent: bool,
) -> list[AllowedOperation]:
    """Build RegisterBindOperation claims from filter variants and bind
    persistence flags.

    :param filters: Filter variants for the bind
    :param persistent: Emit a persistent bind claim
    :param non_persistent: Emit a non-persistent bind claim
    :return: List of register_bind operations
    """
    operations: list[AllowedOperation] = []
    if persistent:
        operations.append(RegisterBindOperation(filters=filters, persistent=True))
    if non_persistent:
        operations.append(RegisterBindOperation(filters=filters, persistent=False))
    return operations


def resolve_bind_persistence(args: argparse.Namespace) -> tuple[bool, bool]:
    """
    Resolve which register_bind persistence variants are requested.

    By default only a persistent bind claim is emitted. --non-persistent
    or --debug replaces it with a non-persistent one. If --persistent is
    also given, the persistent bind is added back alongside.

    :return: (persistent, non_persistent) tuple of requested variants
    """
    non_persistent = args.non_persistent or args.debug
    persistent = (not non_persistent) or args.persistent
    return persistent, non_persistent


def bind_flags_requested(args: argparse.Namespace) -> bool:
    """Check whether any register_bind persistence flag was requested."""
    return any([args.persistent, args.non_persistent, args.debug])


def dedup_and_sort_operations(
    operations: list[AllowedOperation],
) -> list[AllowedOperation]:
    """Deduplicate and sort operations into a deterministic order."""
    seen: set[str] = set()
    deduped: list[AllowedOperation] = []
    for operation in sorted(operations, key=lambda op: op.operation):
        key = operation.model_dump_json()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(operation)
    return deduped


def parse_register_bind_args(raw_binds: list[str] | None) -> list[AllowedOperation]:
    """
    Parse repeatable --register-bind arguments into register_bind operations.

    Each argument must be a JSON object with 'filters' and 'persistent' fields,
    e.g.: --register-bind '{"filters": [{"type": "example.RawFeed"}], "persistent": true}'

    :param raw_binds: List of raw JSON strings provided via --register-bind
    :return: List of register_bind operations
    """
    operations: list[AllowedOperation] = []
    for raw in raw_binds or []:
        try:
            operations.append(RegisterBindOperation.model_validate_json(raw))
        except ValidationError as e:
            raise ValueError(f"--register-bind value is invalid: {raw!r} ({e})") from e
    return operations


def read_passphrase(provided: str | None, *, confirm: bool = False) -> str:
    if provided is not None:
        if not provided:
            raise ValueError("An empty passphrase is not permitted")
        return provided
    passphrase = getpass.getpass("Private key passphrase: ")
    if not passphrase:
        raise ValueError("An empty passphrase is not permitted")
    if confirm:
        confirmation = getpass.getpass("Confirm passphrase: ")
        if passphrase != confirmation:
            raise ValueError("Passphrases do not match")
    return passphrase


def cmd_keypair(args: argparse.Namespace) -> None:
    private_key_path = (
        Path(args.private_key) if args.private_key else DEFAULT_PRIVATE_KEY_PATH
    )
    jwks_is_stdout = args.jwks == "-"
    jwks_path = None
    if not jwks_is_stdout:
        jwks_path = Path(args.jwks) if args.jwks else DEFAULT_JWKS_PATH

    # Warn before overwriting an existing keypair, as this would invalidate
    # all previously issued tokens.
    existing = [
        p for p in (private_key_path, jwks_path) if p is not None and p.exists()
    ]
    if existing and not args.force:
        print(
            "WARNING: regenerating a keypair will overwrite the existing "
            "files and invalidate all previously issued tokens:",
            file=sys.stderr,
        )
        for p in existing:
            print(f"  - {p}", file=sys.stderr)
        confirmation = input("Type 'yes' to continue and overwrite: ").strip()
        if confirmation != "yes":
            print("Aborted.", file=sys.stderr)
            return

    passphrase = read_passphrase(args.passphrase, confirm=True)
    private_pem, jwks = generate_new_keypair(passphrase)

    # Ensure the default keyring directory exists for default paths.
    # Explicit paths are assumed to live in an already-existing directory.
    if args.private_key is None:
        private_key_path.parent.mkdir(parents=True, exist_ok=True)
    if jwks_path is not None and args.jwks is None:
        jwks_path.parent.mkdir(parents=True, exist_ok=True)

    private_key_path.write_bytes(private_pem)
    private_key_path.chmod(0o600)

    jwks_content = json.dumps(jwks, indent=2) + "\n"
    if jwks_path is None:
        sys.stdout.write(jwks_content)
        jwks_destination = "stdout"
    else:
        jwks_path.write_text(jwks_content)
        jwks_destination = str(jwks_path)

    print(
        f"Private key written to {private_key_path}\n"
        f"Public JWKS written to {jwks_destination}",
        file=sys.stderr,
    )


def cmd_issue(args: argparse.Namespace) -> None:
    if args.claims_file is not None:
        claims_raw = Path(args.claims_file).read_text()
    elif not sys.stdin.isatty():
        claims_raw = sys.stdin.read()
    else:
        raise ValueError(
            "No claims provided: pass --claims-file or pipe claims via stdin"
        )

    claims = AuthClaims.model_validate_json(claims_raw)

    passphrase = read_passphrase(args.passphrase)
    private_key_path = (
        Path(args.private_key) if args.private_key else DEFAULT_PRIVATE_KEY_PATH
    )
    private_key = serialization.load_pem_private_key(
        private_key_path.read_bytes(), password=passphrase.encode("utf-8")
    )
    if not isinstance(private_key, ec.EllipticCurvePrivateKey):
        raise TypeError("Private key is not an EC key")

    token = issue_auth_token(
        private_key,
        claims,
        issuer=args.issuer or default_issuer(),
        expire_after_seconds=args.expire_after,
    )
    print(token)


def claims_from_args(args: argparse.Namespace) -> AuthClaims:
    """Build AuthClaims from explicit CLI arguments."""
    raw_filters = args.filters
    filters = parse_filters_args(raw_filters)
    karton_type = args.karton_type

    if karton_type is not None:
        if not args.identity:
            raise ValueError("--identity is required when using --karton-type")
        if karton_type in ("consumer", "karton") and raw_filters is None:
            raise ValueError(
                f"--karton-type {karton_type} requires at least one --filters JSON list"
            )
    elif raw_filters is not None and not args.identity:
        raise ValueError("--identity is required when using --filters")

    foreign_buckets = sorted(set(args.foreign_buckets or []))
    wants_produce = bool(args.produce_task or foreign_buckets)
    wants_consume = bool(args.consume_task)
    wants_log = bool(args.consume_log)
    if karton_type is not None:
        wants_produce = wants_produce or karton_type in ("producer", "karton")
        wants_consume = wants_consume or karton_type in ("consumer", "karton")
        wants_log = wants_log or karton_type == "logconsumer"

    operations: list[AllowedOperation] = []
    if wants_produce:
        operations.append(ProduceTaskOperation(foreign_buckets=foreign_buckets))
    if wants_consume:
        operations.append(ConsumeTaskOperation())
    if raw_filters is not None:
        persistent, non_persistent = resolve_bind_persistence(args)
        operations.extend(
            make_register_bind_operations(filters, persistent, non_persistent)
        )
    operations.extend(parse_register_bind_args(args.register_bind))
    if wants_log:
        operations.append(ConsumeLogOperation())
    if args.debug:
        operations.append(EnableDebugOperation())

    return AuthClaims(
        identity=args.identity,
        allowed_operations=dedup_and_sort_operations(operations),
    )


def cmd_claims(args: argparse.Namespace) -> None:
    if args.karton is not None:
        validate_no_conflicting_args(args)
        karton_class = import_karton_class(args.karton)
        claims = get_claims_from_karton_class(karton_class)
        if bind_flags_requested(args):
            # Rebuild introspected bind claims with the requested
            # persistence variants, keeping the class filters verbatim.
            persistent, non_persistent = resolve_bind_persistence(args)
            new_ops: list[AllowedOperation] = []
            for op in claims.allowed_operations:
                if isinstance(op, RegisterBindOperation):
                    new_ops.extend(
                        make_register_bind_operations(
                            op.filters, persistent, non_persistent
                        )
                    )
                else:
                    new_ops.append(op)
            claims = claims.model_copy(
                update={"allowed_operations": dedup_and_sort_operations(new_ops)}
            )
        if args.debug:
            claims = claims.model_copy(
                update={
                    "allowed_operations": dedup_and_sort_operations(
                        [*claims.allowed_operations, EnableDebugOperation()]
                    )
                }
            )
    elif args.identity is not None:
        claims = claims_from_args(args)
    else:
        raise ValueError(
            "Provide a Karton module/class specifier or use --identity to "
            "build AuthClaims from explicit arguments"
        )
    print(claims.model_dump_json(indent=2))


def validate_no_conflicting_args(args: argparse.Namespace) -> None:
    """Reject explicit-claims flags mixed with a positional module/class.

    The module introspection derives operations from the class itself, so
    operation-defining arguments would be either redundant or ambiguous.
    """
    conflicting = [
        flag
        for flag, value in (
            ("--karton-type", args.karton_type),
            ("--filters", args.filters),
            ("--identity", args.identity),
            ("--produce-task", args.produce_task),
            ("--consume-task", args.consume_task),
            ("--consume-log", args.consume_log),
            ("--register-bind", args.register_bind),
            ("--foreign-buckets", args.foreign_buckets),
        )
        if value
    ]
    if conflicting:
        raise ValueError(
            f"Cannot combine a Karton module/class specifier with "
            f"{', '.join(conflicting)}. The operations are derived from the "
            "class itself"
        )


def cmd_validate(args: argparse.Namespace) -> None:
    if args.jwks == "-":
        jwks_raw = sys.stdin.read()
    else:
        jwks_path = Path(args.jwks) if args.jwks else DEFAULT_JWKS_PATH
        jwks_raw = jwks_path.read_text()
    jwks = json.loads(jwks_raw)
    jwk_set = jwt.PyJWKSet.from_dict(jwks)

    token = args.token
    if token is None:
        token = sys.stdin.read().strip()

    grant = decode_auth_token(token, jwk_set)

    unverified = jwt.decode_complete(token, options={"verify_signature": False})
    payload = unverified["payload"]
    standard_claims = {
        k: payload[k]
        for k in ("sub", "iss", "aud", "iat", "exp", "ver")
        if k in payload
    }

    # Single JSON document: machine-readable (one parsable blob)
    # and human-readable (indented, grouped by section)
    report = {
        "header": unverified["header"],
        "standard_claims": standard_claims,
        "claims": grant.claims.model_dump(mode="json"),
    }
    print(json.dumps(report, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="karton-auth",
        description="Karton authentication key and token management utility",
    )
    subparsers = parser.add_subparsers(dest="command")

    passphrase_parent = argparse.ArgumentParser(add_help=False)
    passphrase_parent.add_argument(
        "--passphrase",
        default=None,
        help="Private key passphrase. Prompted interactively if not provided",
    )

    keypair_parser = subparsers.add_parser(
        "keypair",
        parents=[passphrase_parent],
        help="Generate a new EC P-256 key-pair",
    )
    keypair_parser.add_argument(
        "--private-key",
        default=None,
        help="Output path for the private key PEM (default: ~/.karton/private.pem)",
    )
    keypair_parser.add_argument(
        "--jwks",
        default=None,
        help="Output path for the public JWKS JSON (default: ./jwks.json). "
        "Use '-' to write to stdout",
    )
    keypair_parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite an existing keypair without prompting",
    )
    keypair_parser.set_defaults(func=cmd_keypair)

    issue_parser = subparsers.add_parser(
        "issue",
        parents=[passphrase_parent],
        help="Issue a new token for the given AuthClaims",
    )
    issue_parser.add_argument(
        "--private-key",
        default=None,
        help="Path to the private key PEM (default: ~/.karton/private.pem)",
    )
    issue_parser.add_argument(
        "--issuer",
        default=None,
        help="Token issuer ('iss' claim). Defaults to <user>@<hostname>",
    )
    issue_parser.add_argument(
        "--expire-after",
        type=int,
        default=None,
        help="Token lifetime in seconds (optional)",
    )
    issue_parser.add_argument(
        "--claims-file",
        default=None,
        help="Path to an AuthClaims JSON file. If omitted, claims are read from stdin",
    )
    issue_parser.set_defaults(func=cmd_issue)

    claims_parser = subparsers.add_parser(
        "claims",
        help="Generate AuthClaims from a Karton class or explicit arguments",
    )
    claims_parser.add_argument(
        "karton",
        nargs="?",
        default=None,
        help="Karton module or 'module:ClassName' specifier. "
        "If only a module is given, a single Karton class is auto-detected. "
        "Omit to build claims from explicit arguments instead",
    )
    claims_parser.add_argument(
        "--identity",
        default=None,
        help="Karton service identity. Use this to build claims from explicit "
        "arguments instead of introspecting a Karton class",
    )
    claims_parser.add_argument(
        "--karton-type",
        default=None,
        choices=KARTON_TYPE_CHOICES,
        help="Karton service type (lowercase name of the base class)",
    )
    claims_parser.add_argument(
        "--filters",
        action="append",
        default=None,
        metavar="JSON_LIST",
        help="Bind filters as JSON list of filter variants, e.g. "
        '\'[{"type": "example.RawFeed"}]\'. '
        "Required by --karton-type consumer/karton (an empty list or "
        "'[{}]' is allowed). Repeatable, lists are concatenated",
    )
    claims_parser.add_argument(
        "--persistent",
        action="store_true",
        default=False,
        help="Emit a persistent register_bind claim. By default the bind is "
        "persistent; this flag re-adds it alongside the non-persistent "
        "bind requested by --non-persistent or --debug",
    )
    claims_parser.add_argument(
        "--non-persistent",
        action="store_true",
        default=False,
        help="Replace the persistent register_bind claim with a non-persistent one",
    )
    claims_parser.add_argument(
        "--debug",
        action="store_true",
        default=False,
        help="Allow the service to run in debug mode: adds enable_debug "
        "and implies --non-persistent, as persistent binds are "
        "rejected in debug mode",
    )
    claims_parser.add_argument(
        "--produce-task",
        action="store_true",
        help="Allow the produce_task operation",
    )
    claims_parser.add_argument(
        "--consume-task",
        action="store_true",
        help="Allow the consume_task operation",
    )
    claims_parser.add_argument(
        "--consume-log",
        action="store_true",
        help="Allow the consume_log operation",
    )
    claims_parser.add_argument(
        "--register-bind",
        action="append",
        default=None,
        metavar="JSON",
        help="AllowedRegisterBind claim as JSON object "
        '(\'{"filters": [...], "persistent": true}\'). '
        "Repeatable for multiple binds",
    )
    claims_parser.add_argument(
        "--foreign-buckets",
        action="append",
        default=None,
        metavar="BUCKET",
        help="Foreign bucket name (implies --produce-task). "
        "Repeatable for multiple buckets",
    )
    claims_parser.set_defaults(func=cmd_claims)

    validate_parser = subparsers.add_parser(
        "validate",
        parents=[passphrase_parent],
        help="Validate a token and print its details",
    )
    validate_parser.add_argument(
        "--jwks",
        default=None,
        help="Path to the public JWKS JSON (default: ./jwks.json). "
        "Use '-' to read from stdin",
    )
    validate_parser.add_argument(
        "token",
        nargs="?",
        default=None,
        help="Token to validate. Read from stdin if not provided",
    )
    validate_parser.set_defaults(func=cmd_validate)

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        return
    args.func(args)
