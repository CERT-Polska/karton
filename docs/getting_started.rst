Getting started
===============

Installation and configuration
------------------------------

TBD

Write your first Kartonik
-------------------------

TBD

.. code-block:: python

    from karton.core import Karton
    import json


    class MyFirstKartonik(Karton):
        """
        Just print incoming runnable samples
        """
        identity = "karton.my-first-kartonik"
        filters = [
            {
                "type": "sample",
                "kind": "runnable"
            }
        ]
        persistent = False

        def process(self) -> None:
            headers = self.current_task.headers
            sample = self.current_task.get_resource('sample')

            self.log.info(f"Wild {sample.name} appeared!")
            print(f"Wild {sample.name} appeared!")
            print(json.dumps(headers, indent=4, sort_keys=True))
            print(json.dumps(sample.to_dict(), indent=4, sort_keys=True))
            print(self.current_task.serialize(indent=4))


    if __name__ == "__main__":
        MyFirstKartonik().loop()

