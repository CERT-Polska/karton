from karton.core import Karton, Task, Resource


class ReverserKarton(Karton):
    identity = "karton.reverser"
    filters = [
        {
            "type": "reverse-task"
        }
    ]

    def process(self, task: Task):
        input_file = task.get_resource("file")
        reversed_content = input_file.content[::-1]
        result = Task(
            headers={
                "type": "reverse-result"
            },
            payload={
                "file": Resource("sample.txt", content=reversed_content)
            }
        )
        self.send_task(result)
