import multiprocessing as mp
import os
import sys
from time import sleep, time

import pytest
from karton.core.__version__ import __version__
from karton.core.backend import (
    KartonBackend,
    KartonBackendProtocol,
    KartonBind,
    KartonServiceInfo,
    get_backend,
)
from karton.core import Producer, Config

# Ensure service_runner is importable in spawned children.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


@pytest.fixture(scope="session")
def backend():
    service_info = KartonServiceInfo.create(identity="karton.test-backend")
    return KartonBackend(Config(), service_info=service_info)


@pytest.fixture(scope="session")
def producer():
    return Producer(identity="test-producer")


@pytest.fixture(scope="session")
def direct_producer():
    return Producer(identity="test-producer")


@pytest.fixture(scope="session")
def gateway_producer():
    config = Config(check_sections=False)
    config._config.clear()
    config.set("gateway", "url", "ws://gateway:8000/gateway")
    service_info = KartonServiceInfo.create(identity="test-producer-gateway")
    backend = get_backend(config, service_info=service_info)
    producer = Producer(
        identity="test-producer-gateway", config=config, backend=backend
    )
    yield producer
    backend.close()


@pytest.fixture(scope="session")
def gateway_verifier() -> KartonBackendProtocol:
    """
    Gateway backend that acts as a consumer of ``verify-result`` tasks.

    Consumes result tasks via the gateway WebSocket protocol, ensuring
    that the round-trip (producer -> service -> verifier) goes entirely
    through the public ``KartonBackendProtocol`` interface.
    """
    config = Config(check_sections=False)
    config._config.clear()
    config.set("gateway", "url", "ws://gateway:8000/gateway")
    service_info = KartonServiceInfo.create(identity="karton.test-verifier")
    backend = get_backend(config, service_info=service_info)
    backend.register_bind(KartonBind(
        identity="karton.test-verifier",
        info=None,
        version=__version__,
        filters=[{"type": "verify-result"}],
        persistent=False,
        service_version=None,
        is_async=False,
    ))
    yield backend
    backend.close()


@pytest.fixture(scope="session")
def event_queue():
    return mp.get_context("spawn").Queue()


@pytest.fixture(scope="session")
def services(event_queue: mp.Queue, backend: KartonBackend):
    """
    Start all test service subprocesses, wait for their binds to register
    in Redis, then yield. Terminates on teardown.
    """
    from service_runner import ALL_SERVICE_SPECS, start_service, _service_identity

    processes = []
    for backend_type, instance in ALL_SERVICE_SPECS:
        proc = start_service(backend_type, instance, event_queue)
        processes.append(proc)

    # Wait for all service binds to appear in Redis (startup sync).
    expected_identities = {
        _service_identity(bt, inst) for bt, inst in ALL_SERVICE_SPECS
    }
    deadline = time() + 30
    while time() < deadline:
        binds = backend.get_binds()
        registered = {b.identity for b in binds}
        if expected_identities <= registered:
            break
        sleep(0.2)
    else:
        raise RuntimeError(
            f"Not all service binds registered within 30s. "
            f"Expected: {expected_identities}, Got: {registered}"
        )

    yield

    for proc in processes:
        proc.terminate()
    for proc in processes:
        proc.join(timeout=10)
        if proc.is_alive():
            proc.kill()
            proc.join(timeout=5)
