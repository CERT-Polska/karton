from collections import defaultdict

from fastapi import APIRouter, HTTPException

from karton.core.task import TaskState

from ..backend import gateway_backend
from .models import (
    AnalysisResponseModel,
    QueueListResponseModel,
    QueueResponseModel,
    SuccessResponseModel,
    TaskResponseModel,
)
from .operations import get_queue_info, get_queues_info

api_routes = APIRouter(
    prefix="/api",
)


@api_routes.get("/task/{task_uid}")
async def get_task(task_uid: str):
    task = await gateway_backend.get_task(task_uid=task_uid)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return TaskResponseModel.model_validate(task.serialize())


@api_routes.post("/task/{task_uid}/cancel")
async def cancel_task(task_uid: str):
    task = await gateway_backend.get_task(task_uid=task_uid)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    await gateway_backend.set_task_status(task, TaskState.FINISHED)
    return SuccessResponseModel()


@api_routes.post("/task/{task_uid}/restart")
async def restart_task(task_uid: str):
    task = await gateway_backend.get_task(task_uid=task_uid)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    if task.status is not TaskState.CRASHED:
        raise HTTPException(status_code=403, detail="Task is not in Crashed state")
    new_task = await gateway_backend.restart_task(task)
    return TaskResponseModel.model_validate(new_task.serialize())


@api_routes.get("/analysis/{root_uid}")
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


@api_routes.get("/queue")
async def get_queues():
    queues = await get_queues_info()
    return QueueListResponseModel(
        queues=queues,
    )


@api_routes.get("/queue/{queue_name}")
async def get_queue(queue_name: str):
    queue_info = await get_queue_info(queue_name)
    if not queue_info:
        raise HTTPException(status_code=404, detail="Queue not found")
    return queue_info


@api_routes.post("/queue/{queue_name}/cancel_crashed")
async def cancel_crashed_tasks(queue_name: str): ...


@api_routes.post("/queue/{queue_name}/restart_crashed")
async def restart_crashed_tasks(queue_name: str): ...


@api_routes.get("/service")
async def get_services(): ...


@api_routes.post("/graph")
async def get_graph(): ...
