import pathlib

import jwt
from pydantic import BaseModel, ConfigDict, Field

from karton.core.auth.verifier import load_jwks
from karton.core.config import Config


class GatewayServerConfig(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    secret_key: str = Field(..., min_length=64)
    auth_required: bool
    jwks: jwt.PyJWKSet | None
    allowed_foreign_buckets: list[str]


def get_gateway_config(config: Config) -> GatewayServerConfig:
    secret_key = config.get("gateway-server", "secret_key")
    auth_required = config.getboolean("gateway-server", "auth_required", fallback=True)
    jwks_path = pathlib.Path(config.get("gateway-server", "jwks_path", "jwks.json"))
    if auth_required:
        if not jwks_path.exists():
            raise RuntimeError(
                f"Gateway authentication is enabled (auth_required) but the "
                f"public keys file required to verify auth tokens doesn't "
                f"exist: '{jwks_path}' (looked up relative to the current "
                f"working directory). Generate a keypair and export the "
                f"public keys using 'karton-auth keypair"
                f" --jwks {jwks_path}', or set the 'jwks_path' option in "
                f"the [gateway-server] configuration section to the correct "
                f"file location, or disable authentication by setting "
                f"auth_required = false"
            )
        jwks = load_jwks(jwks_path)
    else:
        jwks = None

    allowed_foreign_buckets = config.get(
        "gateway-server", "allowed_foreign_buckets", ""
    ).split(",")
    return GatewayServerConfig(
        secret_key=secret_key,
        auth_required=auth_required,
        jwks=jwks,
        allowed_foreign_buckets=allowed_foreign_buckets,
    )


karton_config = Config()
gateway_config = get_gateway_config(karton_config)
