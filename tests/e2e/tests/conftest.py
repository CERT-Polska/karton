import pytest
from karton.core.backend import KartonBackend, KartonServiceInfo, get_backend
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
