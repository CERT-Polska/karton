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

    def _meta_field(self, field, default=None):
        if isinstance(self.current_task.payload, dict):
            return self.current_task.payload.get(field, default)
        else:
            return default

    def process(self):
        for resource in self.current_task.resources:
            if resource.name == "sample":
                sample = resource
                break
        else:
            self.log.error("Got task without bound 'sample' resource")
            return

        try:
            fname = self._meta_field("file_name")
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

            task = self.create_task({"type": "sample", "kind": "raw"}, payload={
                "file_name": fname,
                "parent": unpacked.sha256
            })
            resource = self.create_resource("sample", child.contents)
            task.add_resource(resource)
            self.send_task(task)


if __name__ == "__main__":
    conf = Config("config.ini")
    c = Extractor(conf)
    c.loop()
