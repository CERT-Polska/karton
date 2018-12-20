from karton import KartonBaseService


class Ripper(KartonBaseService):
    identity = "karton.ripper"

    def process(self, task):
        raise RuntimeError('fucked up')


if __name__ == "__main__":
    Ripper().loop()
