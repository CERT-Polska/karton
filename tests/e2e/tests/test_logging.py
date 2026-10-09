import pytest
from karton.core import Producer, Task
from karton.core.backend import KartonBackend

from shared import BACKENDS, wait_for_event


@pytest.mark.parametrize("service_backend", BACKENDS)
def test_logging(
    backend: KartonBackend,
    producer: Producer,
    event_queue,
    services,
    service_backend: str,
):
    log_message = "hello this is a test"
    task = Task(
        headers={
            "instance": "first",
            "backend": service_backend,
            "type": "log-task",
            "message": log_message,
        }
    )
    producer.send_task(task)

    event = wait_for_event(
        event_queue,
        event_type="logged",
        uid=task.uid,
        message=log_message,
    )
    assert event["message"] == log_message
