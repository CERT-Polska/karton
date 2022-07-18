from .math_karton import MathKarton
from karton.core import Task
from karton.core.test import KartonTestCase


class MathKartonTestCase(KartonTestCase):
    """Test a karton that accepts an array of integers in "numbers" payload and
    returns their sum in "result".
    """
    karton_class = MathKarton

    def test_addition(self) -> None:
        # prepare a fake test task that matches the production format
        task = Task({
            "type": "math-task",
        }, payload={
            "numbers": [1, 2, 3, 4],
        })

        # dry-run the fake task on the wrapped karton system
        results = self.run_task(task)

        # prepare a expected output task and check if it matches the one produced
        expected_task = Task({
            "origin": "karton.math",
            "type": "math-result"
        }, payload={
            "result": 10,
        })

        self.assertTasksEqual(results, [expected_task])
