import os

from pydantic import BaseModel

from karton.core.config import Config


class GatewayServerConfig(BaseModel):
    secret_key: str
    auth_timeout: int
    auth_required: bool


def get_gateway_config(config: Config) -> GatewayServerConfig:
    secret_key = config.get("gateway-server", "secret_key")
    auth_timeout = config.getint("gateway-server", "auth_timeout", 10)
    auth_required = config.getboolean("gateway-server", "auth_required", True)
    return GatewayServerConfig(
        secret_key=secret_key,
        auth_timeout=auth_timeout,
        auth_required=auth_required,
    )


config_file = os.getenv("KARTON_CONFIG_FILE")
karton_config = Config(config_file)
gateway_config = get_gateway_config(karton_config)
