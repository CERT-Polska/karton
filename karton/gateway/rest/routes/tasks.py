from collections.abc import AsyncIterator

from fastapi import Depends, HTTPException, Response

from karton.core.backend.direct import KARTON_TASK_NAMESPACE
from karton.core.task import Task, TaskState
from karton.gateway.backend import gateway_backend
from karton.gateway.config import gateway_config
from karton.gateway.rest.auth import (
    CanCancelTask,
    CanInspectKarton,
    CanRestartTask,
)
from karton.gateway.rest.models import (
    AnalysisView,
    BatchTaskRequest,
    RestartedTask,
    TaskView,
)
from karton.gateway.task import generate_resource_download_urls

from ._routers import rest_api_router


def _allowed_buckets() -> list[str]:
    return [
        gateway_backend.default_bucket_name,
        *gateway_config.allowed_foreign_buckets,
    ]


@rest_api_router.post("/tasks/batch", dependencies=[Depends(CanInspectKarton)])
async def get_tasks_batch(request: BatchTaskRequest) -> list[TaskView]:
    if len(request.uids) > 10000:
        raise HTTPException(
            status_code=400,
            detail="Maximum number of task UIDs per request is 10000",
        )

    async def _key_iter() -> AsyncIterator[str]:
        for uid in request.uids:
            yield f"{KARTON_TASK_NAMESPACE}:{uid}"

    tasks: list[Task] = []
    async for task in gateway_backend._iter_tasks(
        _key_iter(), chunk_size=1000, parse_resources=False
    ):
        tasks.append(task)
    return [TaskView(**task.to_dict()) for task in tasks]


@rest_api_router.get("/tasks/{uid}", dependencies=[Depends(CanInspectKarton)])
async def get_task_details(uid: str) -> TaskView:
    task = await gateway_backend.get_task(uid)
    if task is None:
        raise HTTPException(status_code=404, detail="Task doesn't exist")
    download_urls = await generate_resource_download_urls(task, _allowed_buckets())
    return TaskView(**task.to_dict(), download_urls=download_urls)


@rest_api_router.get("/analyses/{uid}", dependencies=[Depends(CanInspectKarton)])
async def get_analysis_details(uid: str) -> AnalysisView:
    binds = {bind.identity for bind in await gateway_backend.get_binds()}
    queues: dict[str, list[TaskView]] = {}
    async for task in gateway_backend.iter_task_tree(uid):
        if task.status == TaskState.FINISHED:
            continue
        receiver = task.headers.get("receiver")
        if receiver is None or receiver not in binds:
            continue
        queues.setdefault(receiver, []).append(TaskView(**task.to_dict()))
    return AnalysisView(uid=uid, queues=queues)


@rest_api_router.post("/tasks/{uid}/restart", dependencies=[Depends(CanRestartTask)])
async def restart_task(uid: str) -> RestartedTask:
    task = await gateway_backend.get_task(uid)
    if task is None:
        raise HTTPException(status_code=404, detail="Task doesn't exist")
    new_task = await gateway_backend.restart_task(task)
    return RestartedTask(uid=new_task.uid)


@rest_api_router.post("/tasks/{uid}/cancel", dependencies=[Depends(CanCancelTask)])
async def cancel_task(uid: str) -> Response:
    task = await gateway_backend.get_task(uid)
    if task is None:
        raise HTTPException(status_code=404, detail="Task doesn't exist")
    await gateway_backend.set_task_status(task, TaskState.FINISHED)
    return Response(status_code=204)
