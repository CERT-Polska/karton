import argparse

import pika

from karton import KartonBaseService, Task


class Ripper(KartonBaseService):
    identity = "karton.ripper"

    def process(self):
        counter = int(self.current_task.payload.get("counter", 0)) + 1

        self.log.info("lol my counter is {}".format(self.current_task.payload.get("counter")))

        if counter > 7:
            raise RuntimeError('xDDDDD')

        task = self.current_task.derive_task({"type": "exe"}, [], {"counter": counter})
        self.send_task(task)
        task = self.current_task.derive_task({"type": "exe"}, [], {"counter": counter+3})
        self.send_task(task)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--amqp_url', nargs='?', default='amqp://guest:guest@localhost')

    args = parser.parse_args()

    Ripper(pika.URLParameters(args.amqp_url)).loop()
