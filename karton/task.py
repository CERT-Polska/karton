import json
import uuid

from .resource import ResourceFlagEnum, RemoteDirectoryResource, RemoteResource, PayloadBag


class Task(object):
    def __init__(self, headers, payload=None):
        """
        Task represents some resources + metadata.

        :param headers: Routing information for other systems, this is what allows for evaluation of given \
                        system usefulness for given task. Systems filter by these.
        :param payload: any instance of :py:class:`dict` - contains resources and additional informations
        """
        payload = payload or {}
        if not isinstance(payload, dict):
            raise ValueError("Payload should be an instance of a dict")

        self.uid = str(uuid.uuid4())
        self.root_uid = self.uid
        self.parent_uid = None

        self.headers = headers
        self.payload = PayloadBag()
        self.payload.update(payload)

        self.asynchronic = False

    @classmethod
    def derive_task(cls, headers, task):
        """
        Alternative constructor which copies payload from given task, useful for proxying resource with added metadata.

        :param headers: same as in normal constructor
        :param task: :py:class:`karton.Task` - task to derive from
        :return: :py:class:`karton.Task` with new headers
        """
        new_task = cls(headers=headers, payload=task.payload)
        return new_task

    def set_task_parent(self, parent):
        """
        Bind existing Task to parent task

        :param parent: :py:class:`karton.Task` - task to bind to
        """
        self.parent_uid = parent.uid
        self.root_uid = parent.root_uid
        return self

    def serialize(self):
        resources = {}

        class KartonResourceEncoder(json.JSONEncoder):
            def default(kself, obj):
                if isinstance(obj, RemoteResource):
                    resources[obj.uid] = obj
                    return {"__karton_resource__": obj.to_dict()}
                return json.JSONEncoder.default(kself, obj)

        return json.dumps({"uid": self.uid,
                           "root_uid": self.root_uid,
                           "parent_uid": self.parent_uid,
                           "payload": self.payload},
                          cls=KartonResourceEncoder)

    @staticmethod
    def unserialize(headers, data):
        resources = {}

        def as_resource(resource_dict):
            if '__karton_resource__' in resource_dict:
                karton_resource_dict = resource_dict['__karton_resource__']

                if ResourceFlagEnum.DIRECTORY in karton_resource_dict["flags"]:
                    resource = RemoteDirectoryResource.from_dict(karton_resource_dict)
                else:
                    resource = RemoteResource.from_dict(karton_resource_dict)

                resources[resource.uid] = resource
                return resource
            return resource_dict

        if not isinstance(data, str):
            data = data.decode("utf8")

        data = json.loads(data, object_hook=as_resource)

        task = Task(headers, payload=data["payload"])
        task.uid = data["uid"]
        task.root_uid = data["root_uid"]
        task.parent_uid = data["parent_uid"]
        task.payload.update(resources)
        return task

    def __repr__(self):
        return self.serialize()

    def is_asynchronic(self):
        """
        :return: bool - If current task is asynchronic
        """
        return self.asynchronic

    def make_asynchronic(self):
        """
        Task declares that work will be done by some remote
        handler, so task shouldn't be considered finished when process() returns

        Useful for processing long running tasks - eg. in cuckoo we finish task only after analysis
        """
        self.asynchronic = True

    """
    Following methods are simple wrappers on self.payload PayloadBag for abstracting out direct access.
    Due to decentralized nature of the project this gives us some room for further changes.
    """
    def _add_to_payload(self, name, content):
        if name in self.payload:
            raise ValueError("Payload already exists")

        self.payload[name] = content

    def add_resource(self, name, resource):
        """
        Add resource to task

        :param name: name of the resource
        :param resource: :py:class:`karton.Resource` - resource to be added
        """
        self._add_to_payload(name, resource)

    def add_payload(self, name, content):
        """
        Add payload to task

        :param name: name of the payload
        :param resource: payload to be added
        """
        self._add_to_payload(name, content)

    def get_payload(self, name, default=None):
        """
        Get payload from task

        :param name: name of the payload
        :param default: value to be returned if payload is not present
        :return: payload content
        """
        return self.payload.get(name, default)

    def get_resource(self, name, default=None):
        """
        Get resource from task

        :param name: name of the resource
        :param default: value to be returned if resource is not present
        :return: :py:class:`karton.Resource` - resource with given name
        """
        return self.payload.get(name, default)

    def get_resources(self):
        """
        :return: Generator of all resources present in the :py:class:`karton.PayloadBag`
        """
        return self.payload.resources()

    def get_directory_resources(self):
        """
        :return: Generator of all directory resources present in the :py:class:`karton.PayloadBag`
        """
        return self.payload.directory_resources()

    def get_file_resources(self):
        """
        :return: Generator of all file resources present in the :py:class:`karton.PayloadBag`
        """
        return self.payload.file_resources()

    def remove_payload(self, name):
        """
        Removes payload for the task

        :param name: payload name to be removed
        """
        del self.payload[name]

    def payload_contains(self, name):
        """
        :param name: name of the payload to be checked
        :return: bool - if task's payload contains payload with given name
        """
        return name in self.payload
