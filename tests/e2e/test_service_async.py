from karton.core.asyncio import Karton, Task
from asyncio import sleep
import os


INSTANCE_NAME = os.environ.get("INSTANCE_NAME")
BACKEND = "async"

class TestService(Karton):
    filters = [
        {
            "instance": INSTANCE_NAME,
            "backend": BACKEND,
            "type": "consume-task"
        },
        {
            "instance": INSTANCE_NAME,
            "backend": BACKEND,
            "type": "derive-task"
        },
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
        {
            "instance": INSTANCE_NAME,
            "backend": BACKEND,
            "type": "timeout-task"
        },
        {
            "backend": BACKEND,
            "type": "multiple-sleep-task",
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
            self.send_task(new_task)
        elif task_type in ("sleep-task", "multiple-sleep-task"):
            await sleep(int(task.headers["duration"]))
        elif task_type == "crash-task":
            raise Exception(task.headers["error"])
        elif task_type == "timeout-task":
            if self.task_timeout is not None:
                await sleep(self.task_timeout + 5)


if __name__ == "__main__":
    TestService.main()
