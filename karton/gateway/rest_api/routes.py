from collections import defaultdict

from fastapi import APIRouter, HTTPException

from karton.core.task import TaskState

from ..backend import gateway_backend
from .models import AnalysisResponseModel, SuccessResponseModel, TaskResponseModel, \
    QueueListEntryResponseModel, QueueListResponseModel

rest_api = APIRouter(
    prefix="/api",
)


@rest_api.get("/task/{task_uid}")
async def get_task(task_uid: str):
    task = await gateway_backend.get_task(task_uid=task_uid)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return TaskResponseModel.model_validate(task.serialize())


@rest_api.post("/task/{task_uid}/cancel")
async def cancel_task(task_uid: str):
    task = await gateway_backend.get_task(task_uid=task_uid)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    await gateway_backend.set_task_status(task, TaskState.FINISHED)
    return SuccessResponseModel()


@rest_api.post("/task/{task_uid}/restart")
async def restart_task(task_uid: str):
    task = await gateway_backend.get_task(task_uid=task_uid)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    if task.status is not TaskState.CRASHED:
        raise HTTPException(status_code=403, detail="Task is not in Crashed state")
    new_task = await gateway_backend.restart_task(task)
    return TaskResponseModel.model_validate(new_task.serialize())


@rest_api.get("/analysis/{root_uid}")
async def get_analysis(root_uid: str):
    queues = defaultdict(list)
    async for task in gateway_backend.iter_task_tree(
        root_uid=root_uid, parse_resources=False
    ):
        if task.status is TaskState.FINISHED:
            continue
        receiver = task.receiver if task.receiver else "_unrouted_"
        queues[receiver].append(TaskResponseModel.model_validate(task.serialize()))
    return AnalysisResponseModel(
        uid=root_uid,
        queues=dict(queues),
    )


@rest_api.post("/queue")
async def get_queues():
    binds = await gateway_backend.get_binds()
    services = await gateway_backend.get_online_services()
    queue_counters = {
        bind.identity: QueueListEntryResponseModel(bind=bind)
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
    return QueueListResponseModel(
        queues=queue_counters,
    )


@rest_api.get("/queue/{queue_name}")
async def get_queue(queue_name: str): ...


@rest_api.post("/queue/{queue_name}/cancel_crashed")
async def cancel_crashed_tasks(queue_name: str): ...


@rest_api.post("/queue/{queue_name}/restart_crashed")
async def restart_crashed_tasks(queue_name: str): ...


@rest_api.get("/service")
async def get_services(): ...


@rest_api.post("/graph")
async def get_graph(): ...
