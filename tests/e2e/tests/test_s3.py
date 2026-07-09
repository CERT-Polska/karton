from time import sleep
from itertools import islice
from hashlib import sha256
import os

from shared import (
    BACKENDS,
    wait_for_routed_tasks,
    wait_for_task_state,
)

import pytest
from karton.core import Producer, Consumer, Task, Config
from karton.core.task import TaskState
from karton.core.resource import LocalResource, RemoteResource
from karton.core.backend import KartonBackend


@pytest.mark.parametrize("service_backend", BACKENDS)
def test_resource_upload(
    backend: KartonBackend, producer: Producer, service_backend: str
):
    content = b"Random Resource Content" + os.urandom(2048)
    content_digest = sha256(content).hexdigest()

    task = Task(
        headers={
            "instance": "first",
            "backend": service_backend,
            "type": "sleep-task",
            "duration": 10,
        },
        payload={"resource": LocalResource(name="random.txt", content=content)},
    )
    producer.send_task(task)

    routed_tasks = wait_for_routed_tasks(backend=backend, task_uid=task.uid, timeout=1)
    assert len(routed_tasks) == 1

    routed_task = routed_tasks[0]
    resource_task = wait_for_task_state(
        backend=backend, task_uid=routed_task.uid, state=TaskState.STARTED, timeout=10
    )

    payload = resource_task.get_payload("resource")

    assert isinstance(payload, RemoteResource)

    assert payload.content == content
    assert payload.sha256 == content_digest
