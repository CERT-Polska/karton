import pytest

from shared import (
    BACKENDS,
    wait_for_event,
    wait_for_task_state,
    wait_for_routed_tasks,
)

from karton.core import Producer, Task
from karton.core.task import TaskState
from karton.core.backend import KartonBackend


@pytest.mark.parametrize("service_backend", BACKENDS)
def test_task_crash(
    backend: KartonBackend,
    producer: Producer,
    event_queue,
    services,
    service_backend: str,
):
    error_msg = "hello this is an error"
    task = Task(
        headers={
            "instance": "first",
            "type": "crash-task",
            "error": error_msg,
            "backend": service_backend,
        }
    )
    producer.send_task(task)

    event = wait_for_event(
        event_queue,
        event_type="crashed",
        uid=task.uid,
        error=error_msg,
    )
    assert event["error"] == error_msg


@pytest.mark.parametrize("service_backend", BACKENDS)
def test_task_timeout(
    backend: KartonBackend,
    producer: Producer,
    services,
    service_backend: str,
):
    task = Task(
        headers={
            "instance": "first",
            "type": "timeout-task",
            "backend": service_backend,
        }
    )
    producer.send_task(task)

    routed_tasks = wait_for_routed_tasks(backend=backend, task_uid=task.uid, timeout=1)
    assert len(routed_tasks) == 1

    routed_task = routed_tasks[0]
    crashed_task = wait_for_task_state(
        backend=backend, task_uid=routed_task.uid, state=TaskState.CRASHED, timeout=10
    )

    assert crashed_task.error is not None
    assert "karton.core.exceptions.TaskTimeoutError" in "\n".join(crashed_task.error)
