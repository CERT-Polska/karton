import argparse
import getpass
import importlib
import inspect
import json
import socket
import sys
from pathlib import Path

import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec

from karton.core.asyncio.base import KartonAsyncBase
from karton.core.auth.models import AuthClaims, BindClaim, CapabilityClaim
from karton.core.auth.signer import generate_new_keypair, issue_auth_token
from karton.core.auth.verifier import decode_auth_token
from karton.core.base import KartonBase

DEFAULT_KEYRING_DIR = Path.home() / ".karton"
DEFAULT_PRIVATE_KEY_PATH = DEFAULT_KEYRING_DIR / "private.pem"
DEFAULT_JWKS_PATH = Path("jwks.json")


def default_issuer() -> str:
    return f"{getpass.getuser()}@{socket.gethostname()}"


def get_claims_from_karton_class(
    karton_class: type[KartonBase] | type[KartonAsyncBase],
) -> AuthClaims:
    from karton.core import Consumer, LogConsumer, Producer
    from karton.core.asyncio import Consumer as AsyncConsumer
    from karton.core.asyncio import Producer as AsyncProducer

    claims = AuthClaims(identity=karton_class.identity)

    if issubclass(karton_class, (Producer, AsyncProducer)):
        claims.capabilities.append(CapabilityClaim.produce_task)

    if issubclass(karton_class, (Consumer, AsyncConsumer)):
        claims.capabilities.append(CapabilityClaim.consume_task)
        if claims.binds is None:
            claims.binds = []
        claims.binds.append(
            BindClaim(
                filters=karton_class.filters,
                persistent=karton_class.persistent,
            )
        )

    if issubclass(karton_class, LogConsumer):
        claims.capabilities.append(CapabilityClaim.consume_log)

    return claims


def import_karton_class(dotted: str) -> type[KartonBase] | type[KartonAsyncBase]:
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
    karton_classes: list[type[KartonBase] | type[KartonAsyncBase]] = []
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
        raise ValueError(
            f"No Karton (sub)class found in module {dotted!r}"
        )
    if len(karton_classes) > 1:
        names = ", ".join(c.__name__ for c in karton_classes)
        raise ValueError(
            f"Multiple Karton classes found in module {dotted!r}: {names}. "
            "Specify one using 'module:ClassName'"
        )
    return karton_classes[0]


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
    jwks_path = Path(args.jwks) if args.jwks else DEFAULT_JWKS_PATH

    # Warn before overwriting an existing keypair, as this would invalidate
    # all previously issued tokens.
    existing = [p for p in (private_key_path, jwks_path) if p.exists()]
    if existing and not args.force:
        print(
            "WARNING: regenerating a keypair will overwrite the existing "
            "files and invalidate all previously issued tokens:",
            file=sys.stderr,
        )
        for p in existing:
            print(f"  - {p}", file=sys.stderr)
        confirmation = input(
            "Type 'yes' to continue and overwrite: "
        ).strip()
        if confirmation != "yes":
            print("Aborted.", file=sys.stderr)
            return

    passphrase = read_passphrase(args.passphrase, confirm=True)
    private_pem, jwks = generate_new_keypair(passphrase)

    # Ensure the default keyring directory exists for default paths.
    # Explicit paths are assumed to live in an already-existing directory.
    if args.private_key is None:
        private_key_path.parent.mkdir(parents=True, exist_ok=True)
    if args.jwks is None:
        jwks_path.parent.mkdir(parents=True, exist_ok=True)

    private_key_path.write_bytes(private_pem)
    private_key_path.chmod(0o600)

    jwks_path.write_text(json.dumps(jwks, indent=2) + "\n")

    print(
        f"Private key written to {private_key_path}\n"
        f"Public JWKS written to {jwks_path}",
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
        audience=args.audience,
        expire_after=args.expire_after,
    )
    print(token)


def cmd_claims(args: argparse.Namespace) -> None:
    karton_class = import_karton_class(args.karton)
    claims = get_claims_from_karton_class(karton_class)
    print(claims.model_dump_json(indent=2))


def cmd_validate(args: argparse.Namespace) -> None:
    jwks_path = Path(args.jwks) if args.jwks else DEFAULT_JWKS_PATH
    jwks = json.loads(jwks_path.read_text())
    jwk_set = jwt.PyJWKSet.from_dict(jwks)

    claims = decode_auth_token(args.token, jwk_set, audience=args.audience)

    unverified = jwt.decode_complete(args.token, options={"verify_signature": False})
    header = unverified["header"]
    payload = unverified["payload"]

    print("Header:")
    print(json.dumps(header, indent=2))
    print()
    print("Standard claims:")
    standard = {
        k: payload[k] for k in ("sub", "iss", "aud", "iat", "exp") if k in payload
    }
    print(json.dumps(standard, indent=2, default=str))
    print()
    print("AuthClaims:")
    print(claims.model_dump_json(indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="karton-auth",
        description="Karton authentication key and token management utility",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    passphrase_parent = argparse.ArgumentParser(add_help=False)
    passphrase_parent.add_argument(
        "--passphrase",
        default=None,
        help="Private key passphrase. Prompted interactively if not provided",
    )

    keypair_parser = subparsers.add_parser(
        "keypair", parents=[passphrase_parent],
        help="Generate a new EC P-256 key-pair",
    )
    keypair_parser.add_argument(
        "--private-key",
        default=None,
        help="Output path for the private key PEM "
        "(default: ~/.karton/private.pem)",
    )
    keypair_parser.add_argument(
        "--jwks",
        default=None,
        help="Output path for the public JWKS JSON (default: ./jwks.json)",
    )
    keypair_parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite an existing keypair without prompting",
    )
    keypair_parser.set_defaults(func=cmd_keypair)

    issue_parser = subparsers.add_parser(
        "issue", parents=[passphrase_parent],
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
        "--audience", required=True, help="Token audience ('aud' claim)"
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
        help="Path to an AuthClaims JSON file. "
        "If omitted, claims are read from stdin",
    )
    issue_parser.set_defaults(func=cmd_issue)

    claims_parser = subparsers.add_parser(
        "claims", help="Generate AuthClaims from a Karton class"
    )
    claims_parser.add_argument(
        "karton", help="Karton module or 'module:ClassName' specifier. "
        "If only a module is given, a single Karton class is auto-detected"
    )
    claims_parser.set_defaults(func=cmd_claims)

    validate_parser = subparsers.add_parser(
        "validate", parents=[passphrase_parent],
        help="Validate a token and print its details",
    )
    validate_parser.add_argument(
        "--jwks",
        default=None,
        help="Path to the public JWKS JSON (default: ./jwks.json)",
    )
    validate_parser.add_argument(
        "--audience", required=True, help="Expected token audience ('aud' claim)"
    )
    validate_parser.add_argument("token", help="Token to validate")
    validate_parser.set_defaults(func=cmd_validate)

    args = parser.parse_args()
    args.func(args)
