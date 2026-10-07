import pytest
from karton.core.backend import KartonBackend, KartonServiceInfo
from karton.core import Producer, Config


@pytest.fixture
def backend():
    service_info = KartonServiceInfo.create(identity="karton.test-backend")
    return KartonBackend(Config(), service_info=service_info)


@pytest.fixture
def producer():
    return Producer(identity="test-producer")
