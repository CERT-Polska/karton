from time import sleep
import pytest

from shared import (
    BACKENDS,
    wait_for_task_state,
    wait_for_routed_tasks,
)

from karton.core import Producer, Consumer, Task, Config
from karton.core.task import TaskState
from karton.core.backend import KartonBackend


@pytest.mark.parametrize("service_backend", BACKENDS)
def test_simple_task(backend: KartonBackend, producer: Producer, service_backend: str):
    task = Task(
        headers={
            "instance": "first",
            "backend": service_backend,
            "type": "sleep-task",
            "duration": 5,
        }
    )
    task_id = task.uid

    # task shouldn't exist
    assert backend.get_task(task_id) is None

    producer.send_task(task)
    task_data = backend.get_task(task_id)
    assert task_data is not None
    assert task_data.status is TaskState.DECLARED

    routed_tasks = wait_for_routed_tasks(backend=backend, task_uid=task_id, timeout=1)
    assert (len(routed_tasks)) == 1

    routed_task = routed_tasks[0]
    assert routed_task.status == TaskState.STARTED
    assert routed_task.receiver == f"karton.test-{service_backend}-service-1"

    wait_for_task_state(
        backend=backend, task_uid=routed_task.uid, state=TaskState.FINISHED, timeout=5
    )


@pytest.mark.parametrize("service_backend", BACKENDS)
def test_multiple_routing(
    backend: KartonBackend, producer: Producer, service_backend: str
):
    task = Task(
        headers={
            "type": "multiple-routed-task",
            "duration": 5,
            "backend": service_backend,
        }
    )
    producer.send_task(task)

    routed_tasks = wait_for_routed_tasks(backend=backend, task_uid=task.uid, timeout=1)
    assert (len(routed_tasks)) == 2

    # wait for the tasks to finish
    wait_for_task_state(
        backend=backend,
        task_uid=routed_tasks[0].uid,
        state=TaskState.FINISHED,
        timeout=5,
    )

    routed_tasks = backend.get_tasks([x.uid for x in routed_tasks])
    assert all((x.status == TaskState.FINISHED for x in routed_tasks))
