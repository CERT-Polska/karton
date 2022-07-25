from .multi_reverser_karton import MultiReverserKarton
from karton.core import Task, Resource
from karton.core.test import KartonTestCase


class MultiReverserKartonTestCase(KartonTestCase):
    """
    Test a karton that expects multiple KartonResource in "files" key and spawns a new
    task containing these files reversed.
    """

    karton_class = MultiReverserKarton

    def test_reverse(self) -> None:
        # prepare input data
        input_data = b"foobarbaz"
        # create fake, mini-independent resources
        input_samples = [
            Resource(f"sample{i}.txt", input_data + (b"a"*i))
            for i in range(10)
        ]
        output_samples = [
            Resource(f"sample{i}.txt", (input_data + (b"a" * i))[::-1])
            for i in range(10)
        ]

        # prepare a fake test task that matches the production format
        task = Task({
            "type": "multi-reverse-task",
        }, payload={
            "files": input_samples
        })

        # dry-run the fake task on the wrapped karton system
        results = self.run_task(task)

        # prepare a expected output task and check if it matches the one produced
        expected_task = Task({
            "origin": "karton.multi-reverser",
            "type": "multi-reverse-result"
        }, payload={
            "files": output_samples,
        })

        self.assertTasksEqual(results, [expected_task])
