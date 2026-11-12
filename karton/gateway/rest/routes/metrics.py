from fastapi import Depends

from karton.gateway.backend import gateway_backend
from karton.gateway.rest.auth import (
    CanGetMetrics,
)
from karton.gateway.rest.models import MetricsView

from ._routers import rest_api_router


@rest_api_router.get("/metrics", dependencies=[Depends(CanGetMetrics)])
async def get_metrics() -> MetricsView:
    return MetricsView(metrics=await gateway_backend.get_all_metrics())
