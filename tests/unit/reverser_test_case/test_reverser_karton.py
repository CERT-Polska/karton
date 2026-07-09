from .reverser_karton import ReverserKarton
from karton.core import Task, Resource
from karton.core.test import KartonTestCase


class ReverserKartonTestCase(KartonTestCase):
    """
    Test a karton that expects a KartonResource in "file" key and spawns a new
    task containing that file reversed.
    """

    karton_class = ReverserKarton

    def test_reverse(self) -> None:
        # prepare input data
        input_data = b"foobarbaz"
        # create fake, mini-independent resources
        input_sample = Resource("sample.txt", input_data)
        output_sample = Resource("sample.txt", input_data[::-1])

        # prepare a fake test task that matches the production format
        task = Task({
            "type": "reverse-task",
        }, payload={
            "file": input_sample
        })

        # dry-run the fake task on the wrapped karton system
        results = self.run_task(task)

        # prepare a expected output task and check if it matches the one produced
        expected_task = Task({
            "origin": "karton.reverser",
            "type": "reverse-result"
        }, payload={
            "file": output_sample,
        })

        self.assertTasksEqual(results, [expected_task])
