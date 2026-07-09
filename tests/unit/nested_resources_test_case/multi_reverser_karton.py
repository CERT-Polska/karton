from karton.core import Karton, Task, Resource


class MultiReverserKarton(Karton):
    identity = "karton.multi-reverser"
    filters = [
        {
            "type": "multi-reverse-task"
        }
    ]

    def process(self, task: Task):
        input_files = task.get_payload("files")
        output_files = [
            Resource(file.name, content=file.content[::-1])
            for file in input_files
        ]
        result = Task(
            headers={
                "type": "multi-reverse-result"
            },
            payload={
                "files": output_files
            }
        )
        self.send_task(result)
