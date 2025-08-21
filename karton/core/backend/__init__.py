from karton.core.config import Config

from .base import (
    KartonBackendProtocol,
    KartonBind,
    KartonMetrics,
    KartonServiceInfo,
    KartonServiceType,
)
from .direct import KartonBackend
from .gateway import KartonGatewayBackend


def get_backend(
    config: Config, service_info: KartonServiceInfo
) -> KartonBackendProtocol:
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
    "KartonBackendProtocol",
    "get_backend",
]
