from fastapi import Depends, HTTPException, Response

from karton.core.__version__ import __version__
from karton.core.backend import KartonBind
from karton.gateway.backend import gateway_backend
from karton.gateway.rest.auth import (
    CanInspectKarton,
    CanRemoveBind,
)
from karton.gateway.rest.models import (
    Bind,
)

from ._routers import rest_api_router


@rest_api_router.get("/binds", dependencies=[Depends(CanInspectKarton)])
async def get_binds() -> list[Bind]:
    return [Bind.model_validate(bind) for bind in await gateway_backend.get_binds()]


@rest_api_router.get("/binds/{identity}", dependencies=[Depends(CanInspectKarton)])
async def get_bind_details(identity: str) -> Bind:
    bind = await gateway_backend.get_bind(identity)
    if bind is None:
        raise HTTPException(status_code=404, detail="Bind doesn't exist")
    return Bind.model_validate(bind)


@rest_api_router.delete("/binds/{identity}", dependencies=[Depends(CanRemoveBind)])
async def delete_bind(identity: str) -> Response:
    bind = await gateway_backend.get_bind(identity)
    if bind is None:
        raise HTTPException(status_code=404, detail="Bind doesn't exist")
    replicas = await gateway_backend.get_online_identities()
    if replicas.get(identity):
        raise HTTPException(
            status_code=409,
            detail=(
                "Bind has active replicas that need to be downscaled "
                "before it can be deleted"
            ),
        )
    # Replace the bind with non-persistent bind with empty filters.
    # This is "mark for deletion" for Karton System GC
    tombstone_bind = KartonBind(
        identity=identity,
        info=None,
        version=__version__,
        persistent=False,
        filters=[],
        service_version=None,
        is_async=False,
    )
    await gateway_backend.register_bind(tombstone_bind, bind_backend=False)
    return Response(status_code=204)
