from collections import defaultdict
from typing import Literal

from fastapi import Depends, HTTPException, Response

from karton.core.task import Task, TaskState
from karton.gateway.backend import gateway_backend
from karton.gateway.rest.auth import (
    CanCancelTask,
    CanInspectKarton,
    CanRestartTask,
)
from karton.gateway.rest.models import (
    BulkRestartedTask,
    QueueView,
)

from ._routers import rest_api_router


async def _build_queues() -> dict[str, QueueView]:
    binds = {bind.identity: bind for bind in await gateway_backend.get_binds()}
    replicas = await gateway_backend.get_online_identities()

    pending_uids: dict[str, list[tuple[float, str]]] = defaultdict(list)
    crashed_uids: dict[str, list[tuple[float, str]]] = defaultdict(list)

    # The future planned optimization here is to avoid task deserialization at all
    async for task in gateway_backend.iter_all_tasks(parse_resources=False):
        receiver = task.headers.get("receiver")
        if receiver is None or receiver not in binds:
            continue
        if task.status == TaskState.CRASHED:
            crashed_uids[receiver].append((task.last_update, task.uid))
        elif task.status not in (TaskState.FINISHED, TaskState.CRASHED):
            pending_uids[receiver].append((task.last_update, task.uid))

    queues: dict[str, QueueView] = {}
    for identity, bind in binds.items():
        queue_pending = sorted(
            pending_uids.get(identity, []),
            key=lambda item: item[0],
            reverse=True,
        )
        queue_crashed = sorted(
            crashed_uids.get(identity, []),
            key=lambda item: item[0],
            reverse=True,
        )
        queues[identity] = QueueView(
            identity=bind.identity,
            info=bind.info,
            version=bind.version,
            persistent=bind.persistent,
            filters=bind.filters,
            service_version=bind.service_version,
            is_async=bind.is_async,
            replicas=len(replicas.get(identity, [])),
            pending_task_uids=[uid for _, uid in queue_pending],
            crashed_task_uids=[uid for _, uid in queue_crashed],
        )
    return queues


async def _get_queue_tasks_by_status(identity: str, status: str) -> list[Task]:
    bind = await gateway_backend.get_bind(identity)
    if bind is None:
        raise HTTPException(status_code=404, detail="Queue doesn't exist")

    tasks: list[Task] = []
    async for task in gateway_backend.iter_all_tasks(parse_resources=False):
        if task.headers.get("receiver") != identity:
            continue
        if status == "crashed":
            if task.status == TaskState.CRASHED:
                tasks.append(task)
        elif status == "pending":
            if task.status not in (TaskState.FINISHED, TaskState.CRASHED):
                tasks.append(task)
    return tasks


@rest_api_router.get("/queues", dependencies=[Depends(CanInspectKarton)])
async def get_queues() -> dict[str, QueueView]:
    return await _build_queues()


@rest_api_router.get("/queues/{identity}", dependencies=[Depends(CanInspectKarton)])
async def get_queue_details(identity: str) -> QueueView:
    queues = await _build_queues()
    if identity not in queues:
        raise HTTPException(status_code=404, detail="Queue doesn't exist")
    return queues[identity]


@rest_api_router.post(
    "/queues/{identity}/restart", dependencies=[Depends(CanRestartTask)]
)
async def restart_queue_tasks(
    identity: str,
) -> list[BulkRestartedTask]:
    tasks = await _get_queue_tasks_by_status(identity, "crashed")
    restarted: list[BulkRestartedTask] = []
    for task in tasks:
        new_task = await gateway_backend.restart_task(task)
        restarted.append(BulkRestartedTask(old_uid=task.uid, new_uid=new_task.uid))
    return restarted


@rest_api_router.post(
    "/queues/{identity}/cancel", dependencies=[Depends(CanCancelTask)]
)
async def cancel_queue_tasks(
    identity: str,
    status: Literal["crashed", "pending"] = "crashed",
) -> Response:
    tasks = await _get_queue_tasks_by_status(identity, status)
    for task in tasks:
        await gateway_backend.set_task_status(task, TaskState.FINISHED)
    return Response(status_code=204)
