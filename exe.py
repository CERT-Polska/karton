import argparse

import pika

from karton import KartonBaseService


class Ripper(KartonBaseService):
    identity = "karton.ripper"

    def process(self, task):
        raise RuntimeError('fucked up')


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--amqp_url', nargs='?', default='amqp://guest:guest@localhost')

    args = parser.parse_args()

    Ripper(pika.URLParameters(args.amqp_url)).loop()
