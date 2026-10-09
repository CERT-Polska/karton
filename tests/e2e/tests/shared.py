import multiprocessing as mp
import pytest
from time import sleep, time
from typing import Any, Optional

from karton.core import Producer, Config, Task
from karton.core.backend import KartonBackend, KartonBackendProtocol
from karton.core.task import TaskState


BACKENDS = ["sync", "async"]


def wait_for_event(
    event_queue: mp.Queue,
    event_type: str,
    timeout: int = 10,
    **match: Any,
) -> dict[str, Any]:
    """
    Read events from the multiprocessing queue until one matches the given
    type and key-value predicates. Non-matching events are put back.

    :param event_queue: Multiprocessing queue shared with service subprocesses
    :param event_type: Expected event type (e.g. "logged", "crashed")
    :param timeout: Maximum wait time in seconds
    :param match: Key-value pairs that must be present in the event dict
    :return: The matching event dict
    :raises TimeoutError: If no matching event arrives within timeout
    """
    deadline = time() + timeout
    deferred: list[dict[str, Any]] = []

    while time() < deadline:
        remaining = deadline - time()
        if remaining <= 0:
            break
        try:
            event = event_queue.get(timeout=remaining)
        except Exception:
            break

        if (
            event.get("event") == event_type
            and all(event.get(k) == v for k, v in match.items())
        ):
            for item in deferred:
                event_queue.put(item)
            return event

        deferred.append(event)

    for item in deferred:
        event_queue.put(item)
    raise TimeoutError(
        f"No '{event_type}' event matching {match} received within {timeout}s"
    )


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
    routed_tasks = [
        x for x in analysis_tasks
        if x.receiver is not None and x.parent_uid is None
    ]

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
