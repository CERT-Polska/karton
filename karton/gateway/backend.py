from karton.core.__version__ import __version__
from karton.core.asyncio.backend import KartonAsyncBackend
from karton.core.backend import KartonServiceInfo

from .config import karton_config

gateway_service_info = KartonServiceInfo(
    identity="karton.gateway", karton_version=__version__, service_version=__version__
)
gateway_backend = KartonAsyncBackend(karton_config, service_info=gateway_service_info)
