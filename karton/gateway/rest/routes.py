from collections import defaultdict
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from fastapi.responses import PlainTextResponse

from karton.core.__version__ import __version__
from karton.core.backend import KartonBind, KartonMetrics
from karton.core.task import Task, TaskState
from karton.gateway.backend import gateway_backend
from karton.gateway.config import gateway_config
from karton.gateway.rest.auth import (
    CanCancelTask,
    CanGetMetrics,
    CanInspectKarton,
    CanRemoveBind,
    CanRestartTask,
)
from karton.gateway.rest.models import (
    AnalysisView,
    Bind,
    BulkRestartedTask,
    ProducerOutput,
    QueueView,
    RestartedTask,
    ServiceInfo,
    TaskView,
)
from karton.gateway.task import generate_resource_download_urls

rest_api_router = APIRouter(prefix="/api/v1")
varz_router = APIRouter()


def _allowed_buckets() -> list[str]:
    return [
        gateway_backend.default_bucket_name,
        *gateway_config.allowed_foreign_buckets,
    ]


async def _task_to_view(task: Task) -> TaskView:
    download_urls = await generate_resource_download_urls(task, _allowed_buckets())
    return TaskView(**task.to_dict(), download_urls=download_urls)


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


async def _build_queues() -> dict[str, QueueView]:
    binds = {bind.identity: bind for bind in await gateway_backend.get_binds()}
    replicas = await gateway_backend.get_online_identities()

    pending_uids: dict[str, list[tuple[float, str]]] = defaultdict(list)
    crashed_uids: dict[str, list[tuple[float, str]]] = defaultdict(list)

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
    bind = await gateway_backend.get_bind(identity)
    if bind is None:
        raise HTTPException(status_code=404, detail="Queue doesn't exist")
    replicas = await gateway_backend.get_online_identities()

    pending_uids: list[tuple[float, str]] = []
    crashed_uids: list[tuple[float, str]] = []
    async for task in gateway_backend.iter_all_tasks(parse_resources=False):
        if task.headers.get("receiver") != identity:
            continue
        if task.status == TaskState.CRASHED:
            crashed_uids.append((task.last_update, task.uid))
        elif task.status not in (TaskState.FINISHED, TaskState.CRASHED):
            pending_uids.append((task.last_update, task.uid))

    queue_pending = sorted(pending_uids, key=lambda item: item[0], reverse=True)
    queue_crashed = sorted(crashed_uids, key=lambda item: item[0], reverse=True)
    return QueueView(
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


@rest_api_router.get(
    "/queues/{identity}/tasks", dependencies=[Depends(CanInspectKarton)]
)
async def get_queue_tasks(
    identity: str,
    status: Literal["pending", "crashed"] = "pending",
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
) -> list[TaskView]:
    bind = await gateway_backend.get_bind(identity)
    if bind is None:
        raise HTTPException(status_code=404, detail="Queue doesn't exist")

    matched: list[Task] = []
    async for task in gateway_backend.iter_all_tasks(parse_resources=False):
        if task.headers.get("receiver") != identity:
            continue
        if status == "crashed":
            if task.status != TaskState.CRASHED:
                continue
        else:
            if task.status in (TaskState.FINISHED, TaskState.CRASHED):
                continue
        matched.append(task)

    matched.sort(key=lambda task: task.last_update, reverse=True)
    page = matched[offset : offset + limit]
    return [await _task_to_view(task) for task in page]


@rest_api_router.post(
    "/queues/{identity}/restart", dependencies=[Depends(CanRestartTask)]
)
async def restart_queue_tasks(
    identity: str,
    status: Literal["crashed", "pending"] = "crashed",
) -> list[BulkRestartedTask]:
    tasks = await _get_queue_tasks_by_status(identity, status)
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


@rest_api_router.get("/tasks/{uid}", dependencies=[Depends(CanInspectKarton)])
async def get_task_details(uid: str) -> TaskView:
    task = await gateway_backend.get_task(uid)
    if task is None:
        raise HTTPException(status_code=404, detail="Task doesn't exist")
    return await _task_to_view(task)


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
        queues.setdefault(receiver, []).append(await _task_to_view(task))
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


@rest_api_router.get("/outputs", dependencies=[Depends(CanInspectKarton)])
async def get_outputs() -> list[ProducerOutput]:
    return [
        ProducerOutput.model_validate(output)
        for output in await gateway_backend.get_outputs()
    ]


@varz_router.get("/varz", dependencies=[Depends(CanGetMetrics)])
async def varz() -> PlainTextResponse:
    identities = await gateway_backend.get_online_identities()

    lines = [
        "# HELP karton_replicas Number of online replicas per identity",
        "# TYPE karton_replicas gauge",
    ]
    for identity, services in identities.items():
        lines.append(f'karton_replicas{{identity="{identity}"}} {len(services)}')

    lines += [
        "# HELP karton_tasks Number of tasks per queue, priority and status",
        "# TYPE karton_tasks gauge",
    ]

    task_counts: dict[tuple[str, str, str], int] = {}
    async for task in gateway_backend.iter_all_tasks(parse_resources=False):
        receiver = task.headers.get("receiver", "")
        key = (receiver, task.priority.value, task.status.value)
        task_counts[key] = task_counts.get(key, 0) + 1

    for (queue, priority, status), count in task_counts.items():
        lines.append(
            f'karton_tasks{{queue="{queue}",priority="{priority}",'
            f'status="{status}"}} {count}'
        )

    lines += [
        "# HELP karton_metrics Task counters per identity",
        "# TYPE karton_metrics gauge",
    ]
    for metric in KartonMetrics:
        metric_label = metric.value.split(".")[-1]
        values = await gateway_backend.get_metrics(metric)
        for identity, count in values.items():
            lines.append(
                f'karton_metrics{{metric="{metric_label}",'
                f'identity="{identity}"}} {count}'
            )

    return PlainTextResponse(
        "\n".join(lines) + "\n",
        media_type="text/plain; version=0.0.4; charset=utf-8",
    )
