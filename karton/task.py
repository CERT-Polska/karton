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

        self.uid = str(uuid.uuid4())
        self.root_uid = self.uid
        self.parent_uid = None

        """
        If asynchronic is set: task declares that work will be done by some remote
        handler, so task shouldn't be considered finished when process() returns
        """
        self.asynchronic = False

        self.headers = headers
        self.resources = resources
        self.payload = payload

    def set_task_parent(self, parent):
        """
        Bind existing Task to parent task
        """
        self.parent_uid = parent.uid
        self.root_uid = parent.root_uid
        return self

    def serialize(self):
        return json.dumps({"uid": self.uid,
                           "root_uid": self.root_uid,
                           "parent_uid": self.parent_uid,
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
        task.uid = data["uid"]
        task.root_uid = data["root_uid"]
        task.parent_uid = data["parent_uid"]
        return task

    def get_resource_by_name(self, name, mandatory=True):
        for resource in self.resources:
            if resource.name == name:
                return resource
        else:
            if mandatory:
                raise RuntimeError("Resource {} not found".format(name))
            else:
                return None

    def __repr__(self):
        return self.serialize()

    def add_resource(self, resource):
        self.resources.append(resource)
