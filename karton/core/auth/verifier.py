import jwt

from .models import AuthClaims, BindClaim, CapabilityClaim


def decode_auth_token(
    token: str,
    jwk_set: jwt.PyJWKSet,
    audience: str,
):
    unverified = jwt.decode_complete(token, options={"verify_signature": False})
    header = unverified["header"]
    kid = header.get("kid")
    jwk = jwk_set[kid]
    payload = jwt.decode(
        token,
        key=jwk,
        algorithms=["ES256"],
        options={
            "require": ["aud", "iat", "claims"],
        },
        audience=audience,
    )
    claims = AuthClaims.model_validate(payload["claims"])
    return claims


def verify_auth_claims(
    claims: AuthClaims,
    identity: str,
    required_bind_claim: BindClaim | None = None,
    required_capabilities: list[CapabilityClaim] | None = None,
    required_foreign_buckets: list[str] | None = None,
) -> AuthClaims | None:
    # Identity must be the same
    if claims.identity != identity:
        return None
    if required_bind_claim is not None and claims.binds is not None:
        if all(allowed_bind != required_bind_claim for allowed_bind in claims.binds):
            return None

    if required_capabilities is not None:
        if any(
            required_capability not in claims.capabilities
            for required_capability in required_capabilities
        ):
            return None

    if required_foreign_buckets is not None:
        if any(
            required_foreign_bucket not in claims.foreign_buckets
            for required_foreign_bucket in required_foreign_buckets
        ):
            return None

    return claims
