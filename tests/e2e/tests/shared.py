import pytest
from time import sleep, time
from typing import Optional

from karton.core import Producer, Config, Task
from karton.core.backend import KartonBackend, KartonBackendProtocol
from karton.core.task import TaskState


BACKENDS = ["sync", "async"]


def wait_for_task_state(
    backend: KartonBackend, task_uid: str, state: TaskState, timeout: int
) -> Task:
    poll_start = time()

    while time() - poll_start < timeout:
        task = backend.get_task(task_uid=task_uid)

        if task is None:
            raise Exception(f"Task {task_uid} doesn't exist")

        if task.status == state:
            return task

        sleep(0.2)

    raise TimeoutError(f"Task {task_uid} never changed the state to {state}")


def wait_for_routed_tasks(
    backend: KartonBackend, task_uid: str, timeout: int
) -> list[Task]:
    # wait for the initial task to be routed
    routed_task = wait_for_task_state(
        backend=backend, task_uid=task_uid, state=TaskState.FINISHED, timeout=timeout
    )

    analysis_tasks = list(backend.iter_task_tree(root_uid=routed_task.root_uid))
    routed_tasks = [x for x in analysis_tasks if x.receiver is not None]

    return routed_tasks


def wait_for_result(
    backend: KartonBackendProtocol,
    identity: str = "karton.test-verifier",
    timeout: int = 30,
) -> Task:
    """
    Poll ``consume_routed_task`` on a backend (typed as KartonBackendProtocol)
    until a result task arrives or the timeout is reached.

    Uses only the public protocol interface — no direct Redis/S3 access.
    """
    poll_start = time()
    while time() - poll_start < timeout:
        result = backend.consume_routed_task(identity, timeout=5)
        if result is not None:
            return result
    raise TimeoutError(f"No result task received within {timeout}s")
