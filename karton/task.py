import json
import uuid

from .resource import Resource, DirResource, ResourceFlagEnum


class Task(object):
    def __init__(self, headers, resources=None, payload=None, derive_task=None):
        """
        Create new root Task.
        """
        if resources is None:
            resources = (derive_task and derive_task.resources) or []
        if payload is None:
            payload = (derive_task and derive_task.payload) or {}

        self.uid_stack = [str(uuid.uuid4())]

        self.headers = headers
        self.resources = resources
        self.payload = payload

    @property
    def uid(self):
        return self.uid_stack[-1]

    def bind_task(self, task):
        """
        Bind existing Task to parent task
        """
        self.uid_stack = task.uid_stack + self.uid_stack
        return task

    def serialize(self):
        return json.dumps({"uid_stack": self.uid_stack,
                           "resources": [res.to_dict() for res in self.resources],
                           "payload": self.payload})

    @staticmethod
    def unserialize(headers, data, config=None):
        resources = []
        for resource in data["resources"]:
            if ResourceFlagEnum.DIRECTORY in resource["flags"]:
                r = DirResource._from_dict(resource, config=config)
            else:
                r = Resource._from_dict(resource, config=config)
            resources.append(r)

        task = Task(headers, resources, data["payload"])
        task.uid_stack = data["uid_stack"]
        return task

    def get_resource_by_name(self, name):
        for resource in self.resources:
            if resource.name == name:
                return resource
        else:
            raise RuntimeError("Resource {} not found".format(name))

    def __repr__(self):
        return self.serialize()

    def add_resource(self, resource):
        self.resources.append(resource)

    def _upload_resources(self):
        for resource in self.resources:
            resource._upload()

