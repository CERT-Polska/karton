import sflock
from karton import Karton, Config


class Extractor(Karton):
    identity = "karton.extractor"
    filters = [
        {
            "type": "sample",
            "kind": "archive"
        },
    ]

    def process(self):
        sample = self.current_task.get_resource_by_name("sample")

        try:
            fname = self.current_task.payload.get("file_name")
            if fname:
                fname = fname.encode("utf8")
        except Exception:
            fname = None

        unpacked = sflock.unpack(filename=fname, contents=sample.content)

        try:
            fname = (unpacked.filename and unpacked.filename.decode("utf8")) or unpacked.sha256
        except Exception:
            fname = "(unknown)"

        self.log.info("Got archive {}".format(fname))

        if not unpacked.children:
            self.log.warning("Don't know how to unpack this archive")
            return

        for child in unpacked.children:
            fname = (child.filename and child.filename.decode("utf8")) or child.sha256

            self.log.info("Unpacked child {}".format(fname))

            if not child.contents:
                self.log.warning("Child has no contents")
                continue

            task = self.create_task(
                headers={"type": "sample", "kind": "raw"},
                resource=[
                    self.create_resource("sample", child.contents)
                ],
                payload={
                    "file_name": fname,
                    "parent": unpacked.sha256
                })
            self.send_task(task)


if __name__ == "__main__":
    conf = Config("config.ini")
    c = Extractor(conf)
    c.loop()
