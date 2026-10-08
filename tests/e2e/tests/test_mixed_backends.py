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
from itertools import islice
import os

import pytest

from shared import wait_for_result

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
):
    task = Task(
        headers={
            "type": "multiple-routed-task",
            "duration": 5,
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
    gateway_verifier: KartonBackendProtocol,
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

    logs = gateway_verifier.consume_log(
        timeout=10,
        logger_filter=f"karton.test-{consumer_backend}-service-1",
    )

    mixed_producer.send_task(task)

    service_logs = list(islice(logs, 10))
    for log_record in service_logs:
        if not log_record:
            continue
        exc_text = log_record.get("excText", "")
        if error_msg in exc_text:
            return
    pytest.fail(f"Error message '{error_msg}' not found in logs")


@pytest.mark.parametrize("mixed_producer", PRODUCER_BACKENDS, indirect=True)
@pytest.mark.parametrize("consumer_backend", CONSUMER_BACKENDS)
def test_logging(
    mixed_producer: Producer,
    consumer_backend: str,
    gateway_verifier: KartonBackendProtocol,
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

    logs = gateway_verifier.consume_log(
        timeout=10,
        logger_filter=f"karton.test-{consumer_backend}-service-1",
    )

    mixed_producer.send_task(task)

    service_logs = list(islice(logs, 10))
    messages = [x.get("message") for x in service_logs if x]
    assert log_message in messages


def test_gateway_rejects_foreign_bucket_upload(gateway_producer: Producer):
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
