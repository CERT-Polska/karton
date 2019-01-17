import argparse
import random

import pika

from karton import KartonBaseService, Task, Resource


class Classifier(KartonBaseService):
    identity = "karton.classifier"

    def process(self):
        self.log.info('LOL XD', extra={'tss': random.randint(0, 10)})
        print(self.current_task)
        print([x for x in self.current_task.resources])
        task = Task({"type": "exe"})
        r1 = Resource("analiza", "Analiza")

        task.add_resource(r1)
        self.send_task(task)

        self.send_task(self.current_task.derive_task({"type": "exe"}))
        self.send_task(self.current_task.derive_task({"type": "exe"}))
        self.send_task(self.current_task.derive_task({"type": "exe"}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--amqp-url', nargs='?', default='amqp://guest:guest@localhost')

    args = parser.parse_args()

    Classifier(pika.URLParameters(args.amqp_url)).loop()
