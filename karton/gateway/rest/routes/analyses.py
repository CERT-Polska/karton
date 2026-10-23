from fastapi import Depends

from karton.core.task import TaskState
from karton.gateway.backend import gateway_backend
from karton.gateway.rest.auth import (
    CanInspectKarton,
)
from karton.gateway.rest.models import (
    AnalysisView,
    TaskView,
)

from ._routers import rest_api_router


@rest_api_router.get("/analyses/{uid}", dependencies=[Depends(CanInspectKarton)])
async def get_analysis_details(uid: str) -> AnalysisView:
    binds = {bind.identity for bind in await gateway_backend.get_binds()}
    queues: dict[str, list[TaskView]] = {}
    async for task in gateway_backend.iter_task_tree(uid, parse_resources=False):
        if task.status == TaskState.FINISHED:
            continue
        receiver = task.headers.get("receiver")
        if receiver is None or receiver not in binds:
            continue
        queues.setdefault(receiver, []).append(TaskView(**task.to_dict()))
    return AnalysisView(uid=uid, queues=queues)
