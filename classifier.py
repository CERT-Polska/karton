from karton import Karton, Config


class Classifier(Karton):
    identity = "karton.classifier"
    filters = [
        {
            "type": "sample",
        },
    ]

    def process(self):
        print(self.current_task)

        sample = None
        for resource in self.current_task.resources:
            if resource.name == "sample":
                sample = resource
                break
        else:
            return

        sample_content = sample.content
        print(sample_content)
        if sample_content[:2] == "PE":
            task = self.create_task({"type": "exe"})
            task.add_resource(sample)
            self.send_task(task)


if __name__ == "__main__":
    conf = Config("config.ini")
    c = Classifier(conf)
    c.loop()
