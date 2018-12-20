import argparse
import random

import pika

from karton import KartonBaseService, Task


class Classifier(KartonBaseService):
    identity = "karton.classifier"

    def process(self):
        self.log.info('LOL XD', extra={'tss': random.randint(0, 10)})

        self.send_task(self.current_task.derive_task({"type": "exe"}, [], {}))
        self.send_task(self.current_task.derive_task({"type": "exe"}, [], {}))
        self.send_task(self.current_task.derive_task({"type": "exe"}, [], {}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--amqp-url', nargs='?', default='amqp://guest:guest@localhost')

    args = parser.parse_args()

    Classifier(pika.URLParameters(args.amqp_url)).loop()
