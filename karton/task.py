import itertools
import json
import uuid

from .resource import (
    ResourceFlagEnum,
    RemoteDirectoryResource,
    RemoteResource,
    PayloadBag,
)


class TaskState(object):
    """Enum for task state"""

    DECLARED = "Declared"  # Task declared in TASKS_QUEUE
    SPAWNED = "Spawned"  # Task spawned into subsystem queue
    STARTED = "Started"  # Task is running in subsystem
    FINISHED = "Finished"  # Task finished (ready to forget)


class TaskPriority(object):
    """Enum for priority of tasks"""

    HIGH = "high"
    NORMAL = "normal"
    LOW = "low"


class Task(object):
    """
    Task representation with resources and metadata.

    :param headers: Routing information for other systems, this is what allows for evaluation of given \
                    system usefulness for given task. Systems filter by these.
    :type headers: :py:class:`dict`
    :param payload: any instance of :py:class:`dict` - contains resources and additional informations
    :type payload: :py:class:`dict` or :py:class:`karton.PayloadBag`:
    :param payload_persistent: Persistent payload set for whole task subtree, propagated from root
    :type payload_persistent: :py:class:`dict` or :py:class:`karton.PayloadBag`:
    :param priority: Priority of whole task subtree (propagated like `payload_persistent`)
    :type priority: :class:`TaskPriority`
    :param parent_uid: parent_uid of the task
    :type parent_uid: str
    :param root_uid: root_uid of the task
    :type root_uid: str
    :param uid: uid of the task
    :type uid: str
    """

    def __init__(
        self,
        headers,
        payload=None,
        payload_persistent=None,
        priority=None,
        parent_uid=None,
        root_uid=None,
        uid=None,
    ):
        payload = payload or {}
        payload_persistent = payload_persistent or {}
        if not isinstance(payload, dict):
            raise ValueError("Payload should be an instance of a dict")

        if uid is None:
            self.uid = str(uuid.uuid4())
        else:
            self.uid = uid

        if root_uid is None:
            self.root_uid = self.uid
        else:
            self.root_uid = root_uid

        self.parent_uid = parent_uid

        self.headers = headers
        self.status = TaskState.DECLARED

        self.last_update = None
        self.priority = priority or TaskPriority.NORMAL

        self.payload = PayloadBag()
        self.payload_persistent = PayloadBag()

        self.payload.update(payload)
        self.payload_persistent.update(payload_persistent)

        self.asynchronic = False

    def fork_task(self):
        """
        Fork task to transfer single task to many queues (but use different UID).
        Used internally by karton-system.
        """
        new_task = Task(
            headers=self.headers,
            payload=self.payload,
            payload_persistent=self.payload_persistent,
            priority=self.priority,
            parent_uid=self.parent_uid,
            root_uid=self.root_uid,
        )
        return new_task

    @classmethod
    def derive_task(cls, headers, task):
        """
        Alternative constructor which copies payload from given task, useful for proxying resource with added metadata.

        :param headers: same as in default constructor
        :type headers: :py:class:`dict`
        :param task: task to derive from
        :type task: :py:class:`karton.Task`
        :rtype: :py:class:`karton.Task`
        :return: task with new headers
        """
        new_task = cls(
            headers=headers,
            payload=task.payload,
            payload_persistent=task.payload_persistent,
        )
        return new_task

    def matches_bind(self, bind):
        """
        Checks whether provided task headers are matching filter bind
        :param bind: Filter bind
        :return: True if task matches specific bind
        """
        return all(
            self.headers.get(bind_key) == bind_value
            for bind_key, bind_value in bind.items()
        )

    def set_task_parent(self, parent):
        """
        Bind existing Task to parent task

        :param parent: task to bind to
        :type parent: :py:class:`karton.Task`
        """
        self.parent_uid = parent.uid
        self.root_uid = parent.root_uid
        return self

    def merge_persistent_payload(self, other_task):
        """
        Merge persistent payload from another task

        :param other_task: task to merge persistent payload from
        :type other_task: :py:class:`karton.Task`
        """
        for name, content in other_task.payload_persistent.items():
            self.payload_persistent[name] = content
            if name in self.payload:
                # Delete conflicting non-persistent payload
                del self.payload[name]

    def serialize(self):
        """
        Serialize task data into JSON string
        """

        class KartonResourceEncoder(json.JSONEncoder):
            def default(kself, obj):
                if isinstance(obj, RemoteResource):
                    return {"__karton_resource__": obj.to_dict()}
                return json.JSONEncoder.default(kself, obj)

        return json.dumps(
            {
                "uid": self.uid,
                "root_uid": self.root_uid,
                "parent_uid": self.parent_uid,
                "status": self.status,
                "priority": self.priority,
                "last_update": self.last_update,
                "payload": self.payload,
                "payload_persistent": self.payload_persistent,
                "headers": self.headers,
            },
            cls=KartonResourceEncoder,
        )

    @staticmethod
    def unserialize(data):
        """
        Unserialize Task instance from JSON string
        :param data: JSON-serialized task
        :type data: str or bytes
        """
        if not isinstance(data, str):
            data = data.decode("utf8")

        data = json.loads(data)

        payload = {}
        for k, v in data["payload"].items():
            if isinstance(v, dict) and "__karton_resource__" in v:
                karton_resource_dict = v["__karton_resource__"]

                if ResourceFlagEnum.DIRECTORY in karton_resource_dict["flags"]:
                    resource = RemoteDirectoryResource.from_dict(karton_resource_dict)
                else:
                    resource = RemoteResource.from_dict(karton_resource_dict)

                payload[resource.uid] = resource
                payload[k] = resource
            else:
                payload[k] = v

        payload_persistent = {}
        if "payload_persistent" in data:
            for k, v in data["payload_persistent"].items():
                if isinstance(v, dict) and "__karton_resource__" in v:
                    karton_resource_dict = v["__karton_resource__"]

                    if ResourceFlagEnum.DIRECTORY in karton_resource_dict["flags"]:
                        resource = RemoteDirectoryResource.from_dict(
                            karton_resource_dict
                        )
                    else:
                        resource = RemoteResource.from_dict(karton_resource_dict)

                    payload_persistent[resource.uid] = resource
                    payload_persistent[k] = resource
                else:
                    payload_persistent[k] = v

        task = Task(data["headers"], payload=payload)
        task.uid = data["uid"]
        task.root_uid = data["root_uid"]
        task.parent_uid = data["parent_uid"]
        task.status = data["status"]
        # Backwards compatibility, remove these .get's after upgrade
        task.priority = data.get("priority", TaskPriority.NORMAL)
        task.last_update = data.get("last_update", None)
        if payload_persistent:
            task.payload_persistent = PayloadBag(payload_persistent)
        return task

    def __repr__(self):
        return self.serialize()

    def is_asynchronic(self):
        """
        :rtype: bool
        :return: if current task is asynchronic
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

    def _add_to_payload(self, name, content, persistent=False):
        if name in self.payload:
            raise ValueError("Payload already exists")

        if name in self.payload_persistent:
            raise ValueError("Payload already exists in persistent payloads")

        if not persistent:
            self.payload[name] = content
        else:
            self.payload_persistent[name] = content

    def add_resource(self, name, resource, persistent=False):
        """
        Add resource to task

        :type name: str
        :param name: name of the resource
        :type resource: :py:class:`karton.Resource`
        :param resource: resource to be added
        :type persistent: bool
        :param persistent: flag if the param should be persistent
        """
        self._add_to_payload(name, resource, persistent)

    def add_payload(self, name, content, persistent=False):
        """
        Add payload to task

        :type name: str
        :param name: name of the payload
        :type content: json serializable
        :param content: payload to be added
        :type persistent: bool
        :param persistent: flag if the param should be persistent
        """
        self._add_to_payload(name, content, persistent)

    def get_payload(self, name, default=None):
        """
        Get payload from task

        :type name: str
        :param name: name of the payload
        :type default: object, optional
        :param default: value to be returned if payload is not present
        :return: payload content
        """
        return self.payload.get(name, default) or self.payload_persistent.get(
            name, default
        )

    def get_resource(self, name, default=None):
        """
        Get resource from task

        :param name: name of the resource
        :type name: str
        :param default: value to be returned if resource is not present
        :type default: object, optional
        :return: :py:class:`karton.Resource` - resource with given name
        """
        return self.payload.get(name, default) or self.payload_persistent.get(
            name, default
        )

    def get_resources(self):
        """
        :rtype: Iterator[:py:class:`karton.Resource`]
        :return: Generator of all resources present in the :py:class:`karton.PayloadBag`
        """
        return itertools.chain(
            self.payload.resources(), self.payload_persistent.resources()
        )

    def get_directory_resources(self):
        """
        :rtype: Iterator[:py:class:`karton.DirectoryResource`]
        :return: Generator of all directory resources present in the :py:class:`karton.PayloadBag`
        """
        return itertools.chain(
            self.payload.directory_resources(),
            self.payload_persistent.directory_resources(),
        )

    def get_file_resources(self):
        """
        :rtype: Iterator[:py:class:`karton.Resource`]
        :return: Generator of all file resources present in the :py:class:`karton.PayloadBag`
        """
        return itertools.chain(
            self.payload.file_resources(), self.payload_persistent.file_resources()
        )

    def remove_payload(self, name):
        """
        Removes payload for the task

        :param name: payload name to be removed
        :type name: str
        """
        del self.payload[name]

    def payload_contains(self, name):
        """
        :param name: name of the payload to be checked
        :type name: str
        :rtype: bool
        :return: if task's payload contains payload with given name
        """
        return name in self.payload

    def persistent_payload_contains(self, name):
        """
        :param name: name of the payload to be checked
        :type name: str
        :rtype: bool
        :return: if task's payload contains persistent payload with given name
        """
        return name in self.payload_persistent
