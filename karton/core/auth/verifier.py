import json
import pathlib

import jwt

from .models import DEFAULT_AUDIENCE, AuthClaims


def load_jwks(jwks_path: pathlib.Path) -> jwt.PyJWKSet:
    jwks_raw = jwks_path.read_text()
    jwks = json.loads(jwks_raw)
    return jwt.PyJWKSet.from_dict(jwks)


def decode_auth_token(
    token: str,
    jwk_set: jwt.PyJWKSet,
) -> AuthClaims:
    unverified = jwt.decode_complete(token, options={"verify_signature": False})
    header = unverified["header"]
    kid = header.get("kid")
    jwk = jwk_set[kid]
    payload = jwt.decode(
        token,
        key=jwk,
        algorithms=["ES256"],
        options={
            "require": ["aud", "iat", "jti", "claims"],
        },
        audience=DEFAULT_AUDIENCE,
    )
    claims = AuthClaims.model_validate(payload["claims"])
    return claims
