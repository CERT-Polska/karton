import json
import os
from typing import List

from pydantic import BaseModel, Field

from karton.core.config import Config


class GatewayServerConfig(BaseModel):
    secret_key: str = Field(..., min_length=32)
    allowed_extra_buckets: List[str]


def get_gateway_config(config: Config) -> GatewayServerConfig:
    secret_key = config.get("gateway-server", "secret_key")
    allowed_extra_buckets = json.loads(
        config.get("gateway-server", "allowed_buckets", "[]")
    )
    return GatewayServerConfig(
        secret_key=secret_key,
        allowed_extra_buckets=allowed_extra_buckets,
    )


config_file = os.getenv("KARTON_CONFIG_FILE")
karton_config = Config(config_file)
gateway_config = get_gateway_config(karton_config)
