from fastapi import Depends

from karton.gateway.backend import gateway_backend
from karton.gateway.rest.auth import CanInspectKarton
from karton.gateway.rest.models import ProducerOutput

from ._routers import rest_api_router


@rest_api_router.get("/outputs", dependencies=[Depends(CanInspectKarton)])
async def get_outputs() -> list[ProducerOutput]:
    return [
        ProducerOutput.model_validate(output)
        for output in await gateway_backend.get_outputs()
    ]
