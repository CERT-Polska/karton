import warnings
from typing import Optional

from karton.core.config import Config

from .base import KartonBackendProtocol, KartonBind, KartonMetrics, KartonServiceInfo
from .direct import KartonBackend
from .gateway import KartonGatewayBackend


def get_backend(
    config: Config,
    identity: Optional[str] = None,
    service_info: Optional[KartonServiceInfo] = None,
) -> KartonBackendProtocol:
    if config.has_section("gateway"):
        return KartonGatewayBackend(
            config, identity=identity, service_info=service_info
        )
    else:
        warnings.warn(
            "Direct connection to Redis is deprecated from v6.0.0. "
            "Use Karton Gateway connection instead.",
            DeprecationWarning,
        )
        return KartonBackend(config, identity=identity, service_info=service_info)


__all__ = [
    "KartonBackend",
    "KartonGatewayBackend",
    "KartonBind",
    "KartonMetrics",
    "KartonServiceInfo",
    "KartonBackendProtocol",
    "get_backend",
]
