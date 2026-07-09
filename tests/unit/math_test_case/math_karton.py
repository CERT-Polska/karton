from karton.core import Karton, Task


class MathKarton(Karton):
    identity = "karton.math"
    filters = [
        {
            "type": "math-task"
        }
    ]

    def process(self, task: Task):
        numbers = task.get_payload("numbers")
        sum_of_numbers = sum(numbers)
        result = Task(
            headers={
                "type": "math-result"
            },
            payload={
                "result": sum_of_numbers
            }
        )
        self.send_task(result)
