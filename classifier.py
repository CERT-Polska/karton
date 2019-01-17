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


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--amqp-url', nargs='?', default='amqp://guest:guest@localhost')
    parser.add_argument('--minio-url', nargs='?', default='play.minio.io:9000')
    parser.add_argument('--minio-access-key', nargs='?', default='Q3AM3UQ867SPQQA43P2F')
    parser.add_argument('--minio-secret-key', nargs='?', default='zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG')
    parser.add_argument('--minio-bucket', nargs='?', default='classifier')

    args = parser.parse_args()

    Classifier(pika.URLParameters(args.amqp_url)).loop()
