from __future__ import annotations

import typing

import msgspec

if typing.TYPE_CHECKING:
    from .config import Config
    from .task import Task


# NOTE: those probably should be a core definition of Task/Resources
# that however would be significant change and I didn't want to go to deep with it
# Those definition could be also omitted,
# but I want to use `array_like` to save some space and boost speed


class TaskMsg(msgspec.Struct, array_like=True, kw_only=True, omit_defaults=True):
    uid: str
    root_uid: str
    parent_uid: typing.Optional[str]
    status: str
    priority: str
    payload: dict[str, typing.Any]
    payload_persistent: dict[str, typing.Any]
    headers: dict[str, str]
    # put them at the end so it is easy to omit
    # Compatibility with <= 5.2.0
    headers_persistent: typing.Optional[dict[str, str]] = None
    # Compatibility with <= 3.x.x
    error: typing.Optional[typing.List[str]] = None
    # Compatibility with <= 2.x.x
    last_update: typing.Optional[float] = None
    # Compatibility with <= 3.x.x
    orig_uid: typing.Optional[str] = None


class Serializer:

    def __init__(self):
        self._encoder = msgspec.json.Encoder()
        self._decoder = msgspec.json.Decoder(TaskMsg)

    def setup(self, config: Config):
        if config.get("karton", "format") == "msgpack":
            self._encoder = msgspec.msgpack.Encoder()
            self._decoder = msgspec.msgpack.Decoder(TaskMsg)

    def encode(self, data: typing.Any) -> bytes:
        return self._encoder.encode(data)

    def decode(self, data: bytes) -> typing.Any:
        return self._decoder.decode(data)


serializer = Serializer()


def encode_task(task: Task) -> bytes:
    return serializer.encode(TaskMsg(**task.to_dict()))


def decode_task(task: bytes) -> TaskMsg:
    return serializer.decode(task)
