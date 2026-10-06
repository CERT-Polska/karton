from karton.core.backend import KartonBind, KartonMetrics, KartonServiceInfo
from karton.core.config import Config

from .base import KartonAsyncBackendProtocol
from .direct import KartonAsyncBackend
from .gateway import KartonAsyncGatewayBackend


def get_backend(
    config: Config,
    service_info: KartonServiceInfo,
) -> KartonAsyncBackendProtocol:
    if config.has_section("gateway"):
        return KartonAsyncGatewayBackend(config, service_info=service_info)
    else:
        return KartonAsyncBackend(config, service_info=service_info)


__all__ = [
    "KartonAsyncBackend",
    "KartonAsyncBackendProtocol",
    "KartonBind",
    "KartonMetrics",
    "KartonServiceInfo",
    "get_backend",
]
