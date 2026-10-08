"""
E2E tests for mixed direct/gateway backend setups.

Validates that tasks produced by a direct Producer can be consumed by a
gateway Consumer and vice versa, covering all four combinations:
{direct,gateway} producer x {sync,async,gateway-sync,gateway-async} consumer.
"""
from hashlib import sha256
from itertools import islice
import os

import pytest

from shared import wait_for_task_state, wait_for_routed_tasks

from karton.core import Task
from karton.core.resource import LocalResource, RemoteResource
from karton.core.task import TaskState
from karton.core.backend import KartonBackend

CONSUMER_BACKENDS = ["sync", "async", "gateway-sync", "gateway-async"]
PRODUCER_BACKENDS = ["direct", "gateway"]


@pytest.fixture
def mixed_producer(request, direct_producer, gateway_producer):
    if request.param == "direct":
        return direct_producer
    return gateway_producer


@pytest.mark.parametrize("mixed_producer", PRODUCER_BACKENDS, indirect=True)
@pytest.mark.parametrize("consumer_backend", CONSUMER_BACKENDS)
def test_simple_task(
    mixed_producer, consumer_backend: str, backend: KartonBackend
):
    task = Task(
        headers={
            "instance": "first",
            "backend": consumer_backend,
            "type": "sleep-task",
            "duration": 5,
        }
    )
    task_id = task.uid

    assert backend.get_task(task_id) is None

    mixed_producer.send_task(task)
    task_data = backend.get_task(task_id)
    assert task_data is not None
    assert task_data.status is TaskState.DECLARED

    routed_tasks = wait_for_routed_tasks(
        backend=backend, task_uid=task_id, timeout=1
    )
    assert len(routed_tasks) == 1

    routed_task = routed_tasks[0]
    assert routed_task.status == TaskState.STARTED
    assert routed_task.receiver == f"karton.test-{consumer_backend}-service-1"

    wait_for_task_state(
        backend=backend,
        task_uid=routed_task.uid,
        state=TaskState.FINISHED,
        timeout=5,
    )


@pytest.mark.parametrize("mixed_producer", PRODUCER_BACKENDS, indirect=True)
@pytest.mark.parametrize("consumer_backend", CONSUMER_BACKENDS)
def test_multiple_routing(
    mixed_producer, consumer_backend: str, backend: KartonBackend
):
    task = Task(
        headers={
            "type": "multiple-routed-task",
            "duration": 5,
            "backend": consumer_backend,
        }
    )
    mixed_producer.send_task(task)

    routed_tasks = wait_for_routed_tasks(
        backend=backend, task_uid=task.uid, timeout=1
    )
    assert len(routed_tasks) == 2

    wait_for_task_state(
        backend=backend,
        task_uid=routed_tasks[0].uid,
        state=TaskState.FINISHED,
        timeout=5,
    )

    routed_tasks = backend.get_tasks([x.uid for x in routed_tasks])
    assert all(x.status == TaskState.FINISHED for x in routed_tasks)


@pytest.mark.parametrize("mixed_producer", PRODUCER_BACKENDS, indirect=True)
@pytest.mark.parametrize("consumer_backend", CONSUMER_BACKENDS)
def test_resource_upload(
    mixed_producer, consumer_backend: str, backend: KartonBackend
):
    content = b"Random Resource Content" + os.urandom(2048)
    content_digest = sha256(content).hexdigest()

    task = Task(
        headers={
            "instance": "first",
            "backend": consumer_backend,
            "type": "sleep-task",
            "duration": 10,
        },
        payload={"resource": LocalResource(name="random.txt", content=content)},
    )
    mixed_producer.send_task(task)

    routed_tasks = wait_for_routed_tasks(
        backend=backend, task_uid=task.uid, timeout=1
    )
    assert len(routed_tasks) == 1

    routed_task = routed_tasks[0]
    resource_task = wait_for_task_state(
        backend=backend,
        task_uid=routed_task.uid,
        state=TaskState.STARTED,
        timeout=10,
    )

    payload = resource_task.get_payload("resource")

    assert isinstance(payload, RemoteResource)
    assert payload.content == content
    assert payload.sha256 == content_digest


@pytest.mark.parametrize("mixed_producer", PRODUCER_BACKENDS, indirect=True)
@pytest.mark.parametrize("consumer_backend", CONSUMER_BACKENDS)
def test_task_crash(
    mixed_producer, consumer_backend: str, backend: KartonBackend
):
    error_msg = "hello this is an error"
    task = Task(
        headers={
            "instance": "first",
            "backend": consumer_backend,
            "type": "crash-task",
            "error": error_msg,
        }
    )
    mixed_producer.send_task(task)

    routed_tasks = wait_for_routed_tasks(
        backend=backend, task_uid=task.uid, timeout=1
    )
    assert len(routed_tasks) == 1

    routed_task = routed_tasks[0]

    crashed_task = wait_for_task_state(
        backend=backend,
        task_uid=routed_task.uid,
        state=TaskState.CRASHED,
        timeout=3,
    )
    assert crashed_task.error is not None
    assert error_msg in "\n".join(crashed_task.error)


@pytest.mark.parametrize("mixed_producer", PRODUCER_BACKENDS, indirect=True)
@pytest.mark.parametrize("consumer_backend", CONSUMER_BACKENDS)
def test_logging(
    mixed_producer, consumer_backend: str, backend: KartonBackend
):
    log_message = "hello this is a test"
    task = Task(
        headers={
            "instance": "first",
            "backend": consumer_backend,
            "type": "log-task",
            "message": log_message,
        }
    )

    logs_iterator = backend.consume_log(
        timeout=10, logger_filter=f"karton.test-{consumer_backend}-service-1"
    )

    mixed_producer.send_task(task)

    service_logs = list(islice(logs_iterator, 5))
    messages = [x.get("message") for x in service_logs if x]
    assert log_message in messages


def test_gateway_rejects_foreign_bucket_upload(gateway_producer):
    """
    Gateway backend should reject uploading resources with a custom bucket
    set, client-side, before contacting the server.
    """
    task = Task(
        headers={"backend": "sync", "type": "sleep-task", "duration": 5},
        payload={
            "resource": LocalResource(
                name="foreign.txt",
                content=b"should be rejected",
                bucket="foreign-bucket",
            )
        },
    )
    with pytest.raises(RuntimeError, match="custom buckets"):
        gateway_producer.send_task(task)
