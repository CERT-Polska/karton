from typing import Optional

from karton.core.backend import KartonBind, KartonMetrics, KartonServiceInfo
from karton.core.config import Config

from .base import KartonAsyncBackendProtocol
from .direct import KartonAsyncBackend
from .gateway import KartonGatewayBackend


def get_backend(
    config: Config,
    identity: Optional[str] = None,
    service_info: Optional[KartonServiceInfo] = None,
) -> KartonAsyncBackendProtocol:
    if config.has_section("gateway"):
        return KartonGatewayBackend(
            config, identity=identity, service_info=service_info
        )
    else:
        return KartonAsyncBackend(config, identity=identity, service_info=service_info)


__all__ = [
    "KartonAsyncBackend",
    "KartonAsyncBackendProtocol",
    "KartonBind",
    "KartonMetrics",
    "KartonServiceInfo",
    "get_backend",
]
