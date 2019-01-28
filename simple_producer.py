import sys

from karton import Karton
from config import Config


if __name__ == "__main__":
    c = Karton(Config("config.ini"))
    t = c.create_task({"type": "sample", "kind": "raw"})
    res = c.create_resource("sample", open(sys.argv[1], "rb").read())
    t.add_resource(res)
    c.send_task(t)
    print("Sent")
