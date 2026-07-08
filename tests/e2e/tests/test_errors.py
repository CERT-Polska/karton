from time import sleep
import pytest

from shared import BACKENDS, backend, producer

from karton.core import Producer, Consumer, Task, Config
from karton.core.task import TaskState
from karton.core.backend import KartonBackend


@pytest.mark.parametrize("service_backend", BACKENDS)
def test_task_crash(backend: KartonBackend, producer: Producer, service_backend: str):
    error_msg = "hello this is an error"
    task = Task(headers={"instance": "first", "type":"crash-task", "error": error_msg, "backend": service_backend,})
    producer.send_task(task)

    sleep(2)

    routed_tasks = [x for x in backend.iter_task_tree(root_uid=task.root_uid) if x.receiver is not None]
    assert(len(routed_tasks)) == 1

    routed_task = routed_tasks[0]
    assert routed_task.status == TaskState.CRASHED
    assert routed_task.error is not None
    assert error_msg in "\n".join(routed_task.error)


@pytest.mark.parametrize("service_backend", BACKENDS)
def test_task_timeout(backend: KartonBackend, producer: Producer, service_backend: str):
    task = Task(headers={"instance": "first", "type":"timeout-task", "backend": service_backend})
    producer.send_task(task)

    sleep(1)

    for _ in range(10):
        routed_tasks = [x for x in backend.iter_task_tree(root_uid=task.root_uid) if x.receiver is not None]
        assert(len(routed_tasks)) == 1
        routed_task = routed_tasks[0]

        if routed_task.status != TaskState.STARTED:
            break

        sleep(10)

    assert routed_task.status == TaskState.CRASHED

    assert routed_task.error is not None
    assert "karton.core.exceptions.TaskTimeoutError" in "\n".join(routed_task.error)
