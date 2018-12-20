import argparse

import pika

from karton import KartonBaseService, Task


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--amqp-url', nargs='?', default='amqp://guest:guest@localhost')

    args = parser.parse_args()

    task = Task({"type": "data"}, [], {})
    service = KartonBaseService(pika.URLParameters(args.amqp_url))
    service.send_task(task)
    service.close()

