from karton.core.config import Config

from .base import (
    KartonBackendProtocol,
    KartonBind,
    KartonMetrics,
    KartonServiceInfo,
)
from .direct import KartonBackend


def get_backend(
    config: Config, identity: str, service_info: KartonServiceInfo
) -> KartonBackendProtocol:
    return KartonBackend(config, identity=identity, service_info=service_info)


__all__ = [
    "KartonBackend",
    "KartonBind",
    "KartonMetrics",
    "KartonServiceInfo",
    "KartonServiceType",
    "KartonBackendProtocol",
    "get_backend",
]
