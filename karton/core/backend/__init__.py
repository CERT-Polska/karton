from typing import Optional

from karton.core.config import Config

from .base import KartonBackendProtocol, KartonBind, KartonMetrics, KartonServiceInfo
from .direct import KartonBackend


def get_backend(
    config: Config,
    identity: Optional[str] = None,
    service_info: Optional[KartonServiceInfo] = None,
) -> KartonBackendProtocol:
    return KartonBackend(config, identity=identity, service_info=service_info)


__all__ = [
    "KartonBackend",
    "KartonBind",
    "KartonMetrics",
    "KartonServiceInfo",
    "KartonBackendProtocol",
    "get_backend",
]
