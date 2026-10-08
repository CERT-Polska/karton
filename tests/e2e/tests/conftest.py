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


@pytest.fixture
def backend():
    service_info = KartonServiceInfo.create(identity="karton.test-backend")
    return KartonBackend(Config(), service_info=service_info)


@pytest.fixture
def producer():
    return Producer(identity="test-producer")


@pytest.fixture
def direct_producer():
    return Producer(identity="test-producer")


@pytest.fixture
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


@pytest.fixture
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
