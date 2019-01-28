import sys

from karton import Karton
from config import Config


if __name__ == "__main__":
    conf = Config("config.ini")
    c = Karton(conf)
    t = c.create_task({"type": "analysis", "kind": "cuckoo1"})
    res = c.create_dir_resource("analysis", sys.argv[1])
    t.add_resource(res)
    c.send_task(t)
    print("Sent")
    """
    t = c.create_task({"type": "sample", "kind": "raw"})
    res = c.create_resource("sample", open(sys.argv[1], "rb").read())
    t.add_resource(res)
    c.send_task(t)
    print("Sent")
    """
