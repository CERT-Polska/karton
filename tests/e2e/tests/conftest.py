import pytest
from karton.core.backend import KartonBackend
from karton.core import Producer, Config


@pytest.fixture
def backend():
    return KartonBackend(Config())


@pytest.fixture
def producer():
    return Producer(identity="test-producer")
