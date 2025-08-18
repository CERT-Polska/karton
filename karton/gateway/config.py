from pydantic import BaseModel

from karton.core.config import Config


class GatewayServerConfig(BaseModel):
    secret_key: str
    auth_timeout: int = 10


def get_config(karton_config: Config) -> GatewayServerConfig: ...
