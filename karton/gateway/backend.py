from karton.core.__version__ import __version__
from karton.core.asyncio.backend import KartonAsyncBackend, KartonServiceInfo

from .config import karton_config

gateway_service_info = KartonServiceInfo.create(
    identity="karton.gateway",
    service_version=__version__,
)
gateway_backend = KartonAsyncBackend(karton_config, service_info=gateway_service_info)
