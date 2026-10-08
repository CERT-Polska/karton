"""
Test service runner — spawns Karton consumer subprocesses that report
lifecycle events back to the pytest process via a multiprocessing queue.

Replaces the 8 docker-compose test-service containers with in-process
subprocesses managed by pytest fixtures.
"""
import multiprocessing as mp
import os
import sys
from typing import Any

# Ensure this directory is importable in spawned children (spawn context
# re-imports the module, so __file__-relative imports must resolve).
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

ALL_BACKENDS = ["sync", "async", "gateway-sync", "gateway-async"]
INSTANCES = ["first", "second"]


def _service_identity(backend_type: str, instance: str) -> str:
    return f"karton.test-{backend_type}-service-{1 if instance == 'first' else 2}"


def _build_filters(backend_type: str, instance: str) -> list[dict[str, Any]]:
    instance_name = instance
    return [
        {
            "instance": instance_name,
            "backend": backend_type,
            "type": "log-task",
            "message": "*",
        },
        {
            "instance": instance_name,
            "backend": backend_type,
            "type": "sleep-task",
            "duration": {"$gt": 0},
        },
        {
            "instance": instance_name,
            "backend": backend_type,
            "type": "crash-task",
            "error": "*",
        },
        {"instance": instance_name, "backend": backend_type, "type": "timeout-task"},
        {
            "instance": instance_name,
            "backend": backend_type,
            "type": "verify-task",
        },
        {
            "backend": backend_type,
            "type": "multiple-routed-task",
            "duration": {"$gt": 0},
        },
    ]


def _sync_service_main(
    config_dict: dict[str, Any],
    identity: str,
    backend_type: str,
    instance: str,
    event_queue: mp.Queue,
) -> None:
    from time import sleep as _sleep

    from karton.core import Config, Karton, Task
    from karton.core.resource import RemoteResource

    config = Config(check_sections=False)
    config._config.clear()
    for section, options in config_dict.items():
        for key, value in options.items():
            config.set(section, key, str(value))

    class TestService(Karton):
        filters = _build_filters(backend_type, instance)
        persistent = False
        task_timeout = 5

        def process(self, task: Task) -> None:
            task_type = task.headers["type"]
            orig_uid = task.orig_uid or task.uid
            event_queue.put({"event": "received", "uid": orig_uid, "identity": identity})

            if task_type == "log-task":
                self.log.info(task.headers["message"])
                event_queue.put({
                    "event": "logged",
                    "uid": orig_uid,
                    "identity": identity,
                    "message": task.headers["message"],
                })
            elif task_type in ("sleep-task", "multiple-routed-task"):
                if task_type == "multiple-routed-task":
                    self.send_task(Task(
                        headers={"type": "verify-result", "backend": backend_type},
                    ))
                _sleep(task.headers["duration"])
                event_queue.put({
                    "event": "finished",
                    "uid": orig_uid,
                    "identity": identity,
                })
            elif task_type == "crash-task":
                raise Exception(task.headers["error"])
            elif task_type == "timeout-task":
                _sleep(self.task_timeout + 5)
            elif task_type == "verify-task":
                result_payload: dict[str, Any] = {"original_uid": task.uid}
                if task.has_payload("resource"):
                    resource = task.get_payload("resource")
                    if isinstance(resource, RemoteResource):
                        resource.download()
                    result_payload["sha256"] = resource.sha256
                self.send_task(Task(
                    headers={"type": "verify-result", "backend": backend_type},
                    payload=result_payload,
                ))
                event_queue.put({
                    "event": "verify_done",
                    "uid": orig_uid,
                    "identity": identity,
                })

    service = TestService(config=config, identity=identity)

    def _crash_hook(task, exc):
        if exc is not None:
            orig_uid = task.orig_uid or task.uid
            event_queue.put({
                "event": "crashed",
                "uid": orig_uid,
                "identity": identity,
                "error": str(exc) if exc else "",
            })

    service.add_post_hook(_crash_hook)
    service.loop()


def _async_service_main(
    config_dict: dict[str, Any],
    identity: str,
    backend_type: str,
    instance: str,
    event_queue: mp.Queue,
) -> None:
    import asyncio
    from asyncio import sleep as _asleep

    from karton.core.asyncio import Config, Karton, Task
    from karton.core.asyncio.resource import RemoteResource

    config = Config(check_sections=False)
    config._config.clear()
    for section, options in config_dict.items():
        for key, value in options.items():
            config.set(section, key, str(value))

    class TestService(Karton):
        filters = _build_filters(backend_type, instance)
        persistent = False
        task_timeout = 5

        async def process(self, task: Task) -> None:
            task_type = task.headers["type"]
            orig_uid = task.orig_uid or task.uid
            event_queue.put({"event": "received", "uid": orig_uid, "identity": identity})

            if task_type == "log-task":
                self.log.info(task.headers["message"])
                event_queue.put({
                    "event": "logged",
                    "uid": orig_uid,
                    "identity": identity,
                    "message": task.headers["message"],
                })
            elif task_type in ("sleep-task", "multiple-routed-task"):
                if task_type == "multiple-routed-task":
                    await self.send_task(Task(
                        headers={"type": "verify-result", "backend": backend_type},
                    ))
                await _asleep(task.headers["duration"])
                event_queue.put({
                    "event": "finished",
                    "uid": orig_uid,
                    "identity": identity,
                })
            elif task_type == "crash-task":
                event_queue.put({
                    "event": "crashed",
                    "uid": orig_uid,
                    "identity": identity,
                    "error": task.headers["error"],
                })
                raise Exception(task.headers["error"])
            elif task_type == "timeout-task":
                await _asleep(self.task_timeout + 5)
            elif task_type == "verify-task":
                result_payload: dict[str, Any] = {"original_uid": task.uid}
                if task.has_payload("resource"):
                    resource = task.get_payload("resource")
                    if isinstance(resource, RemoteResource):
                        await resource.download()
                    result_payload["sha256"] = resource.sha256
                await self.send_task(Task(
                    headers={"type": "verify-result", "backend": backend_type},
                    payload=result_payload,
                ))
                event_queue.put({
                    "event": "verify_done",
                    "uid": orig_uid,
                    "identity": identity,
                })

    async def _run():
        service = TestService(config=config, identity=identity)
        await service.loop()

    asyncio.run(_run())


def start_service(
    backend_type: str,
    instance: str,
    event_queue: mp.Queue,
) -> mp.Process:
    ctx = mp.get_context("spawn")
    identity = _service_identity(backend_type, instance)

    if backend_type.startswith("gateway-"):
        config_dict = {"gateway": {"url": "ws://gateway:8000/gateway"}}
    else:
        config_dict = {
            "redis": {"host": "redis", "port": "6379"},
            "s3": {
                "address": "http://silo:9000",
                "access_key": "karton-test-access",
                "secret_key": "karton-test-key",
                "bucket": "karton",
            },
        }

    target = _async_service_main if "async" in backend_type else _sync_service_main

    proc = ctx.Process(
        target=target,
        args=(config_dict, identity, backend_type, instance, event_queue),
        name=identity,
        daemon=True,
    )
    proc.start()
    return proc


ALL_SERVICE_SPECS = [
    (backend, instance)
    for backend in ALL_BACKENDS
    for instance in INSTANCES
]
