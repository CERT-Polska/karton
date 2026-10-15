from fastapi import Depends
from fastapi.responses import PlainTextResponse

from karton.core.backend import KartonMetrics
from karton.gateway.backend import gateway_backend
from karton.gateway.rest.auth import (
    CanGetMetrics,
)

from ._routers import varz_router


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
