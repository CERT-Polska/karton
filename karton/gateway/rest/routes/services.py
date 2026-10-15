from fastapi import Depends

from karton.gateway.backend import gateway_backend
from karton.gateway.rest.auth import CanInspectKarton
from karton.gateway.rest.models import ServiceInfo

from ._routers import rest_api_router


@rest_api_router.get("/services", dependencies=[Depends(CanInspectKarton)])
async def get_services() -> list[ServiceInfo]:
    services = await gateway_backend.get_online_services(_legacy=False)
    counts: dict[tuple[str, str | None, str | None], int] = {}
    for service in services:
        key = (service.identity, service.karton_version, service.service_version)
        counts[key] = counts.get(key, 0) + 1
    return [
        ServiceInfo(
            identity=identity,
            karton_version=karton_version,
            service_version=service_version,
            instance_id=None,
            active_replicas=replicas,
        )
        for (identity, karton_version, service_version), replicas in counts.items()
    ]
