from time import sleep
import pytest

from shared import BACKENDS, backend, producer

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
    root_id = task.root_uid

    # task shouldn't exist
    assert backend.get_task(task_id) is None

    producer.send_task(task)
    task_data = backend.get_task(task_id)
    assert task_data is not None
    assert task_data.status is TaskState.DECLARED

    # give karton system a bit of time to process the task
    sleep(1)

    # the analysis tree should contain the unrouted task and forked task for the service
    analysis_tasks = list(backend.iter_task_tree(root_uid=root_id))

    assert len(analysis_tasks) == 2

    routed_tasks = [x for x in analysis_tasks if x.receiver is not None]
    assert (len(routed_tasks)) == 1

    routed_task = routed_tasks[0]
    assert routed_task.status == TaskState.STARTED
    assert routed_task.receiver == f"karton.test-{service_backend}-service-1"

    # wait for the task to finish
    sleep(5)

    routed_task = backend.get_task(task_uid=routed_task.uid)
    assert routed_task is not None
    assert routed_task.status == TaskState.FINISHED


@pytest.mark.parametrize("service_backend", BACKENDS)
def test_multiple_routing(
    backend: KartonBackend, producer: Producer, service_backend: str
):
    task = Task(
        headers={
            "type": "multiple-sleep-task",
            "duration": 5,
            "backend": service_backend,
        }
    )
    producer.send_task(task)

    # give karton system a bit of time to process the task
    sleep(1)

    analysis_tasks = list(backend.iter_task_tree(root_uid=task.root_uid))
    print(analysis_tasks)

    # analysis tree should contain 2 routed tasks and the original unrouted one
    assert len(analysis_tasks) == 3

    routed_tasks = [x for x in analysis_tasks if x.receiver is not None]
    assert (len(routed_tasks)) == 2

    # wait for the tasks to finish
    sleep(5)

    routed_tasks = backend.get_tasks([x.uid for x in routed_tasks])
    assert all((x.status == TaskState.FINISHED for x in routed_tasks))
