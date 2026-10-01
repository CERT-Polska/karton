import base64
import hashlib
import json

from cryptography.hazmat.primitives.asymmetric import ec


def kid_base64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def get_public_key_params(public_key: ec.EllipticCurvePublicKey) -> dict[str, str]:
    public_numbers = public_key.public_numbers()
    return {
        "crv": "P-256",
        "kty": "EC",
        "x": kid_base64(public_numbers.x.to_bytes(32, "big")),
        "y": kid_base64(public_numbers.y.to_bytes(32, "big")),
    }


def kid_from_public_key(public_key: ec.EllipticCurvePublicKey) -> str:
    # Based on RFC7638
    # https://www.rfc-editor.org/info/rfc7638/#section-3.1
    if not isinstance(public_key.curve, ec.SECP256R1):
        raise TypeError("ES256 requires the P-256 curve")

    params = get_public_key_params(public_key)

    canonical = json.dumps(
        params,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")

    return kid_base64(hashlib.sha256(canonical).digest())


def kid_from_private_key(private_key: ec.EllipticCurvePrivateKey):
    return kid_from_public_key(private_key.public_key())


def public_jwk(public_key: ec.EllipticCurvePublicKey) -> dict[str, str]:
    params = get_public_key_params(public_key)
    kid = kid_from_public_key(public_key)
    return {
        **params,
        "alg": "ES256",
        "use": "sig",
        "kid": kid,
    }
