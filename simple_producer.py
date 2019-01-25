import time

from karton import Karton, Config


if __name__ == "__main__":
    conf = Config("config.ini")
    c = Karton(conf)
    t = c.create_task({"type": "analysis", "origin": "cuckoo"})
    res = c.create_dir_resource("analysis", "/tmp/guwno")
    t.add_resource(res)

    c.send_task(t)
    print("send")
