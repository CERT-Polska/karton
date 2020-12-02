import sys
from karton.core.karton import LogConsumer


class StdoutLogger(LogConsumer):
    identity = "karton.stdout-logger"

    def process_log(self, event: dict):
        print(event["message"], file=sys.stderr)


if __name__ == "__main__":
    StdoutLogger().loop()
