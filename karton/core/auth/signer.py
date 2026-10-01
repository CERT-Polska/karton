import datetime

import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec

from .keys import kid_from_private_key, public_jwk
from .models import AuthClaims


def generate_new_keypair(
    passphrase: str,
):
    private_key = ec.generate_private_key(ec.SECP256R1())
    public_key = private_key.public_key()
    private_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.BestAvailableEncryption(
            passphrase.encode("utf-8")
        ),
    )
    jwk = public_jwk(public_key)
    jwks = {"keys": [jwk]}
    return private_pem, jwks


def issue_auth_token(
    private_key: ec.EllipticCurvePrivateKey,
    claims: AuthClaims,
    issuer: str,
    audience: str,
    expire_after: int | None = None,
):
    kid = kid_from_private_key(private_key)
    issued_at = datetime.datetime.now(datetime.timezone.utc)
    payload = {
        "sub": claims.identity,
        "iss": issuer,
        "aud": audience,
        "iat": issued_at,
        "claims": claims.model_dump(mode="json"),
    }
    if expire_after is not None:
        payload["exp"] = issued_at + datetime.timedelta(seconds=expire_after)

    return jwt.encode(
        payload,
        private_key,
        algorithm="ES256",
        headers={"kid": kid, "typ": "karton-gateway-api-key+jwt"},
    )
