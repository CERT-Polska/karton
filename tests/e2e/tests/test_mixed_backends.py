"""
E2E tests for mixed direct/gateway backend setups.

Validates that tasks produced by a direct Producer can be consumed by a
gateway Consumer and vice versa, covering all four combinations:
{direct,gateway} producer x {sync,async,gateway-sync,gateway-async} consumer.

All verification is done via the public ``KartonBackendProtocol`` interface:
result tasks are consumed through the gateway verifier's ``consume_routed_task``
and ``consume_log`` methods. No direct Redis/S3 access is used for inspection,
so these tests work in a gateway-only deployment.
"""
from hashlib import sha256
import os

import pytest

from shared import wait_for_event, wait_for_result

from karton.core import Producer, Task
from karton.core.backend import KartonBackendProtocol
from karton.core.resource import LocalResource

CONSUMER_BACKENDS = ["sync", "async", "gateway-sync", "gateway-async"]
PRODUCER_BACKENDS = ["direct", "gateway"]


@pytest.fixture
def mixed_producer(request, direct_producer, gateway_producer) -> Producer:
    if request.param == "direct":
        return direct_producer
    return gateway_producer


@pytest.mark.parametrize("mixed_producer", PRODUCER_BACKENDS, indirect=True)
@pytest.mark.parametrize("consumer_backend", CONSUMER_BACKENDS)
def test_simple_task(
    mixed_producer: Producer,
    consumer_backend: str,
    gateway_verifier: KartonBackendProtocol,
    services,
):
    task = Task(
        headers={
            "instance": "first",
            "backend": consumer_backend,
            "type": "verify-task",
        }
    )
    mixed_producer.send_task(task)

    result = wait_for_result(gateway_verifier)
    assert result.get_payload("original_uid") is not None


@pytest.mark.parametrize("mixed_producer", PRODUCER_BACKENDS, indirect=True)
@pytest.mark.parametrize("consumer_backend", CONSUMER_BACKENDS)
def test_multiple_routing(
    mixed_producer: Producer,
    consumer_backend: str,
    gateway_verifier: KartonBackendProtocol,
    services,
):
    task = Task(
        headers={
            "type": "multiple-routed-task",
            "duration": 1,
            "backend": consumer_backend,
        }
    )
    mixed_producer.send_task(task)

    result1 = wait_for_result(gateway_verifier)
    result2 = wait_for_result(gateway_verifier)
    assert result1.uid != result2.uid


@pytest.mark.parametrize("mixed_producer", PRODUCER_BACKENDS, indirect=True)
@pytest.mark.parametrize("consumer_backend", CONSUMER_BACKENDS)
def test_resource_upload(
    mixed_producer: Producer,
    consumer_backend: str,
    gateway_verifier: KartonBackendProtocol,
    services,
):
    content = b"Random Resource Content" + os.urandom(2048)
    content_digest = sha256(content).hexdigest()

    task = Task(
        headers={
            "instance": "first",
            "backend": consumer_backend,
            "type": "verify-task",
        },
        payload={"resource": LocalResource(name="random.txt", content=content)},
    )
    mixed_producer.send_task(task)

    result = wait_for_result(gateway_verifier)
    assert result.get_payload("sha256") == content_digest


@pytest.mark.parametrize("mixed_producer", PRODUCER_BACKENDS, indirect=True)
@pytest.mark.parametrize("consumer_backend", CONSUMER_BACKENDS)
def test_task_crash(
    mixed_producer: Producer,
    consumer_backend: str,
    event_queue,
    services,
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

    event = wait_for_event(
        event_queue,
        event_type="crashed",
        uid=task.uid,
        error=error_msg,
    )
    assert event["error"] == error_msg


@pytest.mark.parametrize("mixed_producer", PRODUCER_BACKENDS, indirect=True)
@pytest.mark.parametrize("consumer_backend", CONSUMER_BACKENDS)
def test_logging(
    mixed_producer: Producer,
    consumer_backend: str,
    event_queue,
    services,
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
    mixed_producer.send_task(task)

    event = wait_for_event(
        event_queue,
        event_type="logged",
        uid=task.uid,
        message=log_message,
    )
    assert event["message"] == log_message


def test_gateway_rejects_foreign_bucket_upload(
    gateway_producer: Producer,
    services,
):
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
