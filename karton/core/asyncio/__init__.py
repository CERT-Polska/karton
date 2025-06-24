import sys

if sys.version_info < (3, 11, 0):
    raise ImportError("karton.core.asyncio is only compatible with Python 3.11+")

from karton.core.config import Config
from karton.core.task import Task

from .karton import Consumer, Karton, Producer
from .resource import LocalResource, RemoteResource, Resource

__all__ = [
    "Karton",
    "Producer",
    "Consumer",
    "Task",
    "Config",
    "LocalResource",
    "Resource",
    "RemoteResource",
]
