from karton.core.config import Config

from .base import (
    KartonBind,
    KartonMetrics,
    KartonServiceInfo,
    KartonServiceType,
    SupportsServiceOperations,
)
from .direct import KartonBackend
from .gateway import KartonGatewayBackend


def get_backend(
    config: Config, service_info: KartonServiceInfo
) -> SupportsServiceOperations:
    if config.has_section("gateway"):
        return KartonGatewayBackend(config, service_info=service_info)
    else:
        return KartonBackend(config, service_info=service_info)


__all__ = [
    "KartonBackend",
    "KartonGatewayBackend",
    "KartonBind",
    "KartonMetrics",
    "KartonServiceInfo",
    "KartonServiceType",
    "SupportsServiceOperations",
    "get_backend",
]
