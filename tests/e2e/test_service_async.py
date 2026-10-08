from karton.core.asyncio import Karton, Task
from karton.core.asyncio.resource import RemoteResource
from asyncio import sleep
import os


INSTANCE_NAME = os.environ["INSTANCE_NAME"]
BACKEND = os.environ.get("BACKEND_TYPE", "async")


class TestService(Karton):
    filters = [
        {"instance": INSTANCE_NAME, "backend": BACKEND, "type": "consume-task"},
        {"instance": INSTANCE_NAME, "backend": BACKEND, "type": "derive-task"},
        {
            "instance": INSTANCE_NAME,
            "backend": BACKEND,
            "type": "log-task",
            "message": "*",
        },
        {
            "instance": INSTANCE_NAME,
            "backend": BACKEND,
            "type": "sleep-task",
            "duration": {"$gt": 0},
        },
        {
            "instance": INSTANCE_NAME,
            "backend": BACKEND,
            "type": "crash-task",
            "error": "*",
        },
        {"instance": INSTANCE_NAME, "backend": BACKEND, "type": "timeout-task"},
        {
            "instance": INSTANCE_NAME,
            "backend": BACKEND,
            "type": "verify-task",
        },
        {
            "backend": BACKEND,
            "type": "multiple-routed-task",
            "duration": {"$gt": 0},
        },
    ]

    async def process(self, task: Task):
        task_type = task.headers["type"]

        if task_type == "consume-task":
            pass
        elif task_type == "log-task":
            self.log.info(task.headers["message"])
        elif task_type == "derive-task":
            new_task = Task(headers={"type": "derived-task"})
            await self.send_task(new_task)
        elif task_type in ("sleep-task", "multiple-routed-task"):
            if task_type == "multiple-routed-task":
                await self.send_task(Task(
                    headers={"type": "verify-result", "backend": BACKEND},
                ))
            await sleep(task.headers["duration"])
        elif task_type == "crash-task":
            raise Exception(task.headers["error"])
        elif task_type == "timeout-task":
            if self.task_timeout is None:
                raise Exception("Cannot timeout because task_timeout is not set")

            await sleep(self.task_timeout + 5)
        elif task_type == "verify-task":
            result_payload = {"original_uid": task.uid}
            if task.has_payload("resource"):
                resource = task.get_payload("resource")
                if isinstance(resource, RemoteResource):
                    await resource.download()
                result_payload["sha256"] = resource.sha256
            await self.send_task(Task(
                headers={"type": "verify-result", "backend": BACKEND},
                payload=result_payload,
            ))


if __name__ == "__main__":
    TestService.main()
