import pytest

from karton.core import Producer, Config
from karton.core.backend import KartonBackend


BACKENDS = ["sync", "async"]


@pytest.fixture
def backend():
    return KartonBackend(Config())


@pytest.fixture
def producer():
    return Producer(identity="test-producer")
