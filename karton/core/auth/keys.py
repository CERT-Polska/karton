import base64
import hashlib
import json

import jwt.algorithms
from cryptography.hazmat.primitives.asymmetric import ec


def encode_base64url(data: bytes) -> str:
    """
    Encode bytes using the Base64url encoding of JWS (RFC7515)

    See also https://datatracker.ietf.org/doc/html/rfc7515#appendix-A.2.1

    :param data: Bytes to be encoded
    :return: Base64url representation
    """
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def get_base_jwk_from_public_key(
    public_key: ec.EllipticCurvePublicKey,
) -> dict[str, str]:
    """
    Serialize ECDSA P-256 public key into JWK

    :param public_key: ECDSA P-256 public key
    :return: JWK dict object
    """
    if not isinstance(public_key.curve, ec.SECP256R1):
        raise TypeError("ES256 requires the P-256 curve")
    return jwt.algorithms.ECAlgorithm.to_jwk(public_key, as_dict=True)


def get_jwk_thumbprint(public_key: ec.EllipticCurvePublicKey) -> str:
    """
    Evaluate a JWK thumbprint for provided ECDSA P-256 public key

    Based on RFC7638 (https://www.rfc-editor.org/info/rfc7638/#section-3.1)

    :param public_key: ECDSA P-256 public key
    :return: JWK SHA-256 Thumbprint value
    """
    params = get_base_jwk_from_public_key(public_key)

    canonical = json.dumps(
        params,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")

    return encode_base64url(hashlib.sha256(canonical).digest())


def get_jwk_thumbprint_from_private_key(private_key: ec.EllipticCurvePrivateKey):
    """
    Evaluate a JWK Thumbprint for provided ECDSA P-256 private key

    :param private_key: ECDSA P-256 private key
    :return: JWK SHA-256 Thumbprint value
    """
    return get_jwk_thumbprint(private_key.public_key())


def get_jwk_from_public_key(public_key: ec.EllipticCurvePublicKey) -> dict[str, str]:
    """
    Serializes ECDSA P-256 public key into JWK with evaluated
    'kid', 'use' and 'alg' values, so it can be used in JWKS.

    Parameters based on https://www.rfc-editor.org/info/rfc7517/#section-4

    :param public_key: ECDSA P-256 public key

    """
    params = get_base_jwk_from_public_key(public_key)
    kid = get_jwk_thumbprint(public_key)
    return {
        **params,
        "alg": "ES256",
        "use": "sig",
        "kid": kid,
    }
