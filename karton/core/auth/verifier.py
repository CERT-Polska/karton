import json
import pathlib

import jwt

from .models import (
    DEFAULT_AUDIENCE,
    SUPPORTED_TOKEN_VERSIONS,
    AuthClaims,
    VerifiedGrant,
)


class UnsupportedTokenVersionError(jwt.InvalidTokenError):
    """Raised when a token's `ver` claim is not in SUPPORTED_TOKEN_VERSIONS."""


def load_jwks(jwks_path: pathlib.Path) -> jwt.PyJWKSet:
    jwks_raw = jwks_path.read_text()
    jwks = json.loads(jwks_raw)
    return jwt.PyJWKSet.from_dict(jwks)


def decode_auth_token(
    token: str,
    jwk_set: jwt.PyJWKSet,
) -> VerifiedGrant:
    unverified = jwt.decode_complete(token, options={"verify_signature": False})
    header = unverified["header"]
    kid = header.get("kid")
    jwk = jwk_set[kid]
    payload = jwt.decode(
        token,
        key=jwk,
        algorithms=["ES256"],
        options={
            "require": ["aud", "iat", "jti", "ver", "claims"],
        },
        audience=DEFAULT_AUDIENCE,
    )
    # We may want to change the ACL semantics in the future
    # and keep some compatibility with older tokens.
    # In that case: the mapping should be done here.
    token_version = payload["ver"]
    if token_version not in SUPPORTED_TOKEN_VERSIONS:
        raise UnsupportedTokenVersionError(
            f"Unsupported token version: {token_version}. "
            f"Supported: {sorted(SUPPORTED_TOKEN_VERSIONS)}"
        )
    claims = AuthClaims.model_validate(payload["claims"])
    return VerifiedGrant(
        claims=claims,
        expires_at=payload.get("exp"),
        token_id=payload["jti"],
    )
