import textwrap
from datetime import datetime
from pathlib import Path

import mistune  # type: ignore
from fastapi import APIRouter, HTTPException, Request
from fastapi.templating import Jinja2Templates
from starlette.responses import HTMLResponse

from .operations import (
    get_queue_info,
    get_queues_info,
    get_services_info,
    get_task_info,
)

package_dir = Path(__file__).parent
routes = APIRouter()
templates = Jinja2Templates(directory=package_dir / "templates")

markdown = mistune.create_markdown(
    escape=True,
    renderer="html",
    plugins=["url", "strikethrough", "footnotes", "table"],
)


def render_description(description: str) -> str | None:
    if not description:
        return None
    return markdown(textwrap.dedent(description))


def render_timestamp(timestamp: float) -> str:
    return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")


def pretty_delta(timestamp: float) -> str:
    diff = datetime.now() - datetime.fromtimestamp(timestamp)
    seconds_diff = int(diff.total_seconds())
    if seconds_diff < 180:
        return f"{seconds_diff} seconds ago"
    minutes_diff = seconds_diff // 60
    if minutes_diff < 180:
        return f"{minutes_diff} minutes ago"
    hours_diff = minutes_diff // 60
    return f"{hours_diff} hours ago"


def short_task_uid(task_uid: str) -> str:
    if "}:" in task_uid:
        return task_uid.split("}:")[1]
    else:
        return task_uid


templates.env.filters["render_description"] = render_description
templates.env.filters["render_timestamp"] = render_timestamp
templates.env.filters["pretty_delta"] = pretty_delta
templates.env.filters["short_task_uid"] = short_task_uid


@routes.get("/", response_class=HTMLResponse)
async def index(request: Request):
    queues = await get_queues_info()
    return templates.TemplateResponse(
        "index.html", context={"request": request, "queues": queues}
    )


@routes.get("/queue/{queue_name}", response_class=HTMLResponse)
async def get_queue(request: Request, queue_name: str):
    queue = await get_queue_info(queue_name)
    if not queue:
        raise HTTPException(status_code=404, detail="Queue not found")
    return templates.TemplateResponse(
        "queue.html", context={"request": request, "queue": queue}
    )


@routes.get("/queue/{queue_name}/crashed", response_class=HTMLResponse)
async def get_crashed_queue(request: Request, queue_name: str):
    queue = await get_queue_info(queue_name)
    if not queue:
        raise HTTPException(status_code=404, detail="Queue not found")
    return templates.TemplateResponse(
        "crashed.html", context={"request": request, "queue": queue}
    )


@routes.get("/services", response_class=HTMLResponse)
async def get_services(request: Request):
    services = await get_services_info()
    return templates.TemplateResponse(
        "services.html", context={"request": request, "services": services}
    )


@routes.get("/graph", response_class=HTMLResponse)
async def get_graph(request: Request):
    return templates.TemplateResponse("layout.html", context={"request": request})


@routes.get("/task/{task_id}", response_class=HTMLResponse)
async def get_task(request: Request, task_id: str):
    task = await get_task_info(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return templates.TemplateResponse(
        "task.html", context={"request": request, "task": task}
    )


@routes.post("/task/{task_id}/cancel", response_class=HTMLResponse)
async def cancel_task(request: Request, task_id: str):
    return templates.TemplateResponse("layout.html", context={"request": request})


@routes.post("/task/{task_id}/restart", response_class=HTMLResponse)
async def restart_task(request: Request, task_id: str):
    return templates.TemplateResponse("layout.html", context={"request": request})


@routes.get("/analysis/{root_id}", response_class=HTMLResponse)
async def get_analysis(request: Request, root_id: str):
    return templates.TemplateResponse("layout.html", context={"request": request})


@routes.post("/queue/{queue_name}/cancel_crashed")
async def cancel_crashed_queue_tasks(request: Request, queue_name: str):
    return templates.TemplateResponse("layout.html", context={"request": request})


@routes.post("/queue/{queue_name}/cancel_pending")
async def cancel_pending_queue_tasks(request: Request, queue_name: str):
    return templates.TemplateResponse("layout.html", context={"request": request})


@routes.post("/queue/{queue_name}/restart_crashed")
async def restart_crashed_queue_tasks(request: Request, queue_name: str):
    return templates.TemplateResponse("layout.html", context={"request": request})
