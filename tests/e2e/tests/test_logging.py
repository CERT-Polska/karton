from time import sleep
from itertools import islice

from shared import (
    BACKENDS,
    wait_for_routed_tasks,
    wait_for_task_state,
)

import pytest
from karton.core import Producer, Consumer, Task, Config
from karton.core.task import TaskState
from karton.core.backend import KartonBackend


@pytest.mark.parametrize("service_backend", BACKENDS)
def test_logging(backend: KartonBackend, producer: Producer, service_backend: str):
    log_message = "hello this is a test"
    task = Task(
        headers={
            "instance": "first",
            "backend": service_backend,
            "type": "log-task",
            "message": log_message,
        }
    )

    logs_iterator = backend.consume_log(
        timeout=10, logger_filter=f"karton.test-{service_backend}-service-1"
    )

    producer.send_task(task)

    service_logs = list(islice(logs_iterator, 5))
    messages = [x.get("message") for x in service_logs if x]
    assert log_message in messages
