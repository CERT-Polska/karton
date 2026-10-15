import importlib
import pkgutil

from ._routers import rest_api_router, varz_router

# Import every submodule so route decorators register on the routers above.
for _module_info in pkgutil.iter_modules(__path__):
    if not _module_info.name.startswith("_"):
        importlib.import_module(f"{__name__}.{_module_info.name}")

__all__ = ["rest_api_router", "varz_router"]
