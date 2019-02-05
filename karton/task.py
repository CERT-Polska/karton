import json
import uuid

from .resource import Resource, DirResource, ResourceFlagEnum


class Task(object):
    def __init__(self, headers, payload=None, derive_task=None):
        """
        Create new root Task.
        """
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

        self.resources = {}
        self.headers = headers
        self.payload = payload

    def set_task_parent(self, parent):
        """
        Bind existing Task to parent task
        """
        self.parent_uid = parent.uid
        self.root_uid = parent.root_uid
        return self

    def serialize(self):
        class KartonResourceEncoder(json.JSONEncoder):
            def default(self, obj):
                if isinstance(obj, Resource):
                    obj.upload()
                    return {"__karton_resource__": obj.to_dict()}
                return json.JSONEncoder.default(self, obj)

        return json.dumps({"uid": self.uid,
                           "root_uid": self.root_uid,
                           "parent_uid": self.parent_uid,
                           "payload": self.payload},
                          cls=KartonResourceEncoder)

    @staticmethod
    def unserialize(headers, data, config=None):
        resources = {}

        def as_resource(dct):
            if '__karton_resource__' in dct:
                resource = dct['__karton_resource__']
                resource = (DirResource if ResourceFlagEnum.DIRECTORY in resource["flags"] else Resource)\
                    .from_dict(resource, config=config)
                resources[resource.uid] = resource
                return resource
            return dct

        data = json.loads(data, object_hook=as_resource)

        task = Task(headers, payload=data["payload"])
        task.uid = data["uid"]
        task.root_uid = data["root_uid"]
        task.parent_uid = data["parent_uid"]
        task.resources = resources
        return task

    def __repr__(self):
        return self.serialize()
