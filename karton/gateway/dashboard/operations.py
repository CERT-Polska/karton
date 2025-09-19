from collections import defaultdict

from karton.core.backend import KartonServiceInfo
from karton.core.task import TaskState

from ..backend import gateway_backend
from .models import (
    QueueListEntryResponseModel,
    QueueResponseModel,
    ServicesListEntryResponseModel,
    TaskResponseModel,
)


async def get_services_count() -> dict[KartonServiceInfo, int]:
    services = await gateway_backend.get_online_services()
    counters = defaultdict(int)
    for service in services:
        if service.instance_id is None:
            # Every connection without instance_id
            # count as separate replica
            counters[service] += 1
        else:
            # For connections with instance_id: count
            # unique instance_id under instance_id=None
            if not counters[service]:
                service_key = KartonServiceInfo(
                    identity=service.identity,
                    karton_version=service.karton_version,
                    service_version=service.service_version,
                    instance_id=None,
                )
                counters[service_key] += 1
            counters[service] += 1
    return {srv: count for srv, count in counters.items() if srv.instance_id is None}


async def get_queues_info() -> dict[str, QueueListEntryResponseModel]:
    binds = await gateway_backend.get_binds()
    services_count = await get_services_count()
    queue_counters = {
        bind.identity: QueueListEntryResponseModel(
            bind=bind,
            active_consumers=services_count.get(
                KartonServiceInfo(
                    identity=bind.identity,
                    karton_version=bind.version,
                    service_version=bind.service_version,
                ),
                0,
            ),
        )
        for bind in binds
    }
    async for task in gateway_backend.iter_all_tasks(parse_resources=False):
        if task.status is TaskState.FINISHED:
            continue
        queue = task.receiver
        if not queue or queue not in queue_counters:
            continue
        if task.status is not TaskState.CRASHED:
            queue_counters[queue].pending_tasks += 1
        else:
            queue_counters[queue].crashed_tasks += 1
    return queue_counters


async def get_queue_info(queue_name: str) -> QueueResponseModel | None:
    bind = await gateway_backend.get_bind(queue_name)
    if not bind:
        return None

    services_count = await get_services_count()
    active_consumers = services_count.get(
        KartonServiceInfo(
            identity=bind.identity,
            karton_version=bind.version,
            service_version=bind.service_version,
        ),
        0,
    )
    pending_tasks: list[TaskResponseModel] = []
    crashed_tasks: list[TaskResponseModel] = []
    async for task in gateway_backend.iter_all_tasks(parse_resources=False):
        if task.status is TaskState.FINISHED:
            continue
        queue = task.receiver
        if queue != queue_name:
            continue
        task_obj = TaskResponseModel.model_validate({**task.to_dict(), "task": task})
        if task.status is TaskState.CRASHED:
            crashed_tasks.append(task_obj)
        else:
            pending_tasks.append(task_obj)
    return QueueResponseModel(
        bind=bind,
        pending_tasks=pending_tasks,
        crashed_tasks=crashed_tasks,
        active_consumers=active_consumers,
    )


async def get_services_info() -> list[ServicesListEntryResponseModel]:
    services_count = await get_services_count()
    services = sorted(services_count.keys(), key=lambda s: s.identity)
    return [
        ServicesListEntryResponseModel(
            identity=service.identity,
            karton_version=service.karton_version,
            service_version=service.service_version,
            active_instances=services_count[service],
        )
        for service in services
    ]


async def get_task_info(task_id: str) -> TaskResponseModel | None:
    task = await gateway_backend.get_task(task_id)
    if not task:
        return None
    return TaskResponseModel.model_validate({**task.to_dict(), "task": task})
