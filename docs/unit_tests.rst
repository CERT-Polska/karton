Writing unit tests
==================

Basic unit test
---------------

So you want to test your karton systems, that's great! The karton core actually comes with a few helper methods to make it a bit easier.

The building block of all karton tests is :py:meth:`karton.core.test.KartonTestCase`.
It's a nifty class that wraps around your karton system and allows you to run tasks on it without needing to create a producer.
What's more important however, is that it runs without any Redis or MinIO interaction and thus creates no side effects.

.. code-block:: python

    from math_karton import MathKarton
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
            expected_task = Task(
                "type": "math-result"
            }, payload={
                "result": 10,
            })

            self.assertTasksEqual(results, expected_task)


Testing resources
-----------------

That was pretty simple, but what about testing karton systems that accept and spawn payloads containing resources?

:py:meth:`karton.core.test.KartonTestCase` already takes care of them for you. Just use normal :py:meth:`karton.core.Resource` like you would normally do.


.. code-block:: python

    from reverse import ReverserKarton
    from karton.core.test import KartonTestCase
    from karton.core import Resource

    class ReverserKartonTestCase(KartonTestCase):
        """Test a karton that expects a KartonResource in "file" key and spawns a new
        task containing that file reversed.
        """

        karton_class = ReverserKarton

        def test_reverse(self) -> None:
            # load data from testcase files
            with open("testdata/file.txt", "rb") as f:
                input_data = f.read()
            
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
            expected_task = Task(
                "type": "reverse-result"
            }, payload={
                "file": output_sample,
            })

            self.assertTasksEqual(results, expected_task)
