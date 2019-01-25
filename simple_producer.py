import time

from karton import Karton, Config


if __name__ == "__main__":
    conf = Config("config.ini")
    c = Karton(conf)
    for i in range(1000):
        t = c.create_task({"type": "sample", "kind": "dunnoh"})
        if i % 2 == 0:
            res = c.create_resource("sample", b"PE asdasdasd")
        else:
            res = c.create_resource("sample", b"asdasdasdasd")
        t.add_resource(res)
        print(t)
        c.send_task(t)
        print("send")
        time.sleep(3)
