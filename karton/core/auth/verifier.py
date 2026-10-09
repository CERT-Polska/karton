import json
import pathlib
from typing import cast

import jwt
from jwt import InvalidTokenError

from .models import (
    DEFAULT_AUDIENCE,
    SUPPORTED_TOKEN_VERSIONS,
    AuthClaims,
    JWTPayload,
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
    """
    Decode and validate provided token against provided JWKS

    :param token: Authorization token to decode
    :param jwk_set: Parsed JWKS with public keys for signature verification
    :return: VerifiedGrant object describing authorization grants
    """
    unverified = jwt.decode_complete(token, options={"verify_signature": False})
    header = unverified["header"]
    if not header.get("kid"):
        raise InvalidTokenError("'kid' not found in provided authorization token")
    kid = header.get("kid")
    if kid not in jwk_set:
        raise InvalidTokenError("Token is signed using unknown public key")
    jwk = jwk_set[kid]
    payload = cast(
        JWTPayload,
        jwt.decode(
            token,
            key=jwk,
            algorithms=["ES256"],
            options={
                "require": list(JWTPayload.__required_keys__),
            },
            audience=DEFAULT_AUDIENCE,
        ),
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
