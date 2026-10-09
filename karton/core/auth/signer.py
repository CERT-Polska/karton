import datetime
import uuid

import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec

from .keys import get_jwk_from_public_key, get_jwk_thumbprint_from_private_key
from .models import DEFAULT_AUDIENCE, TOKEN_VERSION, AuthClaims, JWKSDict


def generate_new_keypair(
    passphrase: str,
) -> tuple[bytes, JWKSDict]:
    """
    Generate a new keypair for signing Karton authorization tokens

    :param passphrase: Passphrase for private key encryption
    :return: tuple with PEM-serialized private key and JWKS-serialized public key
    """
    private_key = ec.generate_private_key(ec.SECP256R1())
    public_key = private_key.public_key()
    private_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.BestAvailableEncryption(
            passphrase.encode("utf-8")
        ),
    )
    jwks: JWKSDict = {"keys": [get_jwk_from_public_key(public_key)]}
    return private_pem, jwks


def issue_auth_token(
    private_key: ec.EllipticCurvePrivateKey,
    claims: AuthClaims,
    issuer: str,
    expire_after: int | None = None,
):
    kid = get_jwk_thumbprint_from_private_key(private_key)
    issued_at = datetime.datetime.now(datetime.timezone.utc)
    payload = {
        "sub": claims.identity,
        "iss": issuer,
        "aud": DEFAULT_AUDIENCE,
        "iat": issued_at,
        "jti": str(uuid.uuid4()),
        "ver": TOKEN_VERSION,
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
