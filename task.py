import json
import uuid

from resource import Resource


class Task:
    def __init__(self, headers, resources=None, payload=None):
        """
        Create new root Task.
        """
        if resources is None:
            resources = []
        if payload is None:
            payload = []

        self.uid_stack = [str(uuid.uuid4())]

        self.headers = headers
        self.resources = resources
        self.payload = payload

    @property
    def uid(self):
        return self.uid_stack[-1]

    def derive_task(self, task):
        """
        Derive existing Task which is a child of this Task.
        """
        task.uid_stack = self.uid_stack + task.uid_stack
        return task

    def derive_new_task(self, headers=None, resources=None, payload=None):
        """
        Derive new Task which is a child of this Task.
        """
        task = Task(headers or self.headers, resources or self.resources, payload or self.payload)
        task.uid_stack = self.uid_stack + task.uid_stack
        return task

    def serialize(self):
        return json.dumps({"uid_stack": self.uid_stack,
                           "resources": [res.to_dict() for res in self.resources],
                           "payload": self.payload})

    @staticmethod
    def unserialize(headers, data, config=None):
        resources = [Resource._from_dict(x, config=config) for x in data["resources"]]

        task = Task(headers, resources, data["payload"])
        task.uid_stack = data["uid_stack"]
        return task

    def __repr__(self):
        return self.serialize()

    def add_resource(self, resource):
        self.resources.append(resource)

    def _upload_resources(self):
        for resource in self.resources:
            resource._upload()

