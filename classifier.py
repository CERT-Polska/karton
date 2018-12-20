import random

from karton import KartonBaseService, Task


class Classifier(KartonBaseService):
    identity = "karton.classifier"

    def process(self, task):
        self.log.info('LOL XD', extra={'tss': random.randint(0, 10)})
        self.send_task(Task({"type": "exe"}, []))


if __name__ == "__main__":
    Classifier().loop()
