from karton.core import Karton, Task
from time import sleep
import os


INSTANCE_NAME = os.environ["INSTANCE_NAME"]
BACKEND = "sync"


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
            "backend": BACKEND,
            "type": "multiple-routed-task",
            "duration": {"$gt": 0},
        },
    ]

    def process(self, task: Task):
        task_type = task.headers["type"]

        if task_type == "consume-task":
            pass
        elif task_type == "log-task":
            self.log.info(task.headers["message"])
        elif task_type == "derive-task":
            new_task = Task(headers={"type": "derived-task"})
            self.send_task(new_task)
        elif task_type in ("sleep-task", "multiple-routed-task"):
            sleep(task.headers["duration"])
        elif task_type == "crash-task":
            raise Exception(task.headers["error"])
        elif task_type == "timeout-task":
            if self.task_timeout is None:
                raise Exception("Cannot timeout because task_timeout is not set")

            sleep(self.task_timeout + 5)


if __name__ == "__main__":
    TestService.main()
