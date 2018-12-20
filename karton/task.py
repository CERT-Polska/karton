import json
import uuid

from .resource import ResourceBase, remote_resource_from_dict


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
    :type payload: :py:class:`dict`
    :param payload_persistent: Persistent payload set for whole task subtree, propagated from initial task
    :type payload_persistent: :py:class:`dict`
    :param priority: Priority of whole task subtree (propagated from initial task like `payload_persistent`)
    :type priority: :class:`TaskPriority`
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
        if not isinstance(payload_persistent, dict):
            raise ValueError("Persistent payload should be an instance of a dict")

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

        self.payload = dict(payload)
        self.payload_persistent = dict(payload_persistent)

        self.asynchronic = False

    def fork_task(self):
        """
        Fork task to transfer single task to many queues (but use different UID).

        Used internally by karton-system

        :meta private:
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

    def derive_task(self, headers):
        """
        Creates copy of task with different headers, useful for proxying resource with added metadata.

        .. code-block:: python

            class MZClassifier(Karton):
                identity = "karton.mz-classifier"
                filters = {
                    "type": "sample",
                    "kind": "raw"
                }

                def process(self):
                    sample = self.current_task.get_resource("sample")
                    if sample.content.startswith(b"MZ"):
                        self.log.info("MZ detected!")
                        task = self.current_task.derive_task({
                            "type": "sample",
                            "kind": "exe"
                        })
                        self.send_task(task)
                    self.log.info("Not a MZ :<")

        .. versionchanged:: 3.0.0

            Moved from static method to regular method:

            :code:`Task.derive_task(headers, task)` must be ported to :code:`task.derive_task(headers)`

        :param headers: new headers for task
        :type headers: :py:class:`dict`
        :rtype: :py:class:`karton.Task`
        :return: copy of task with new headers
        """
        new_task = Task(
            headers=headers,
            payload=self.payload,
            payload_persistent=self.payload_persistent,
        )
        return new_task

    def matches_bind(self, bind):
        """
        Checks whether provided task headers are matching filter bind

        :param bind: Filter bind
        :return: True if task matches specific bind

        :meta private:
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

        :meta private:
        """
        self.parent_uid = parent.uid
        self.root_uid = parent.root_uid
        return self

    def merge_persistent_payload(self, other_task):
        """
        Merge persistent payload from another task

        :param other_task: task to merge persistent payload from
        :type other_task: :py:class:`karton.Task`

        :meta private:
        """
        for name, content in other_task.payload_persistent.items():
            self.payload_persistent[name] = content
            if name in self.payload:
                # Delete conflicting non-persistent payload
                del self.payload[name]

    def serialize(self):
        """
        Serialize task data into JSON string

        :meta private:
        """

        class KartonResourceEncoder(json.JSONEncoder):
            def default(kself, obj):
                if isinstance(obj, ResourceBase):
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

    def walk_payload_bags(self):
        """
        Iterate over all payload bags and payloads contained in them

        Generates tuples (payload_bag, key, value)

        :rtype: Iterator[Tuple[Dict[str, Any], str, Any]]
        """
        for payload_bag in [self.payload, self.payload_persistent]:
            for key, value in payload_bag.items():
                yield payload_bag, key, value

    def iterate_resources(self):
        """
        Get list of resource objects bound to Task

        Generates tuples (key, value)

        :rtype: Iterator[Tuple[str, ResourceBase]]
        """
        for _, key, value in self.walk_payload_bags():
            if isinstance(value, ResourceBase):
                yield key, value

    def unserialize_resources(self, minio):
        """
        Transforms __karton_resource__ serialized entries into
        Remote(Directory)Resource object instances

        :meta private:
        """
        for payload_bag, key, value in self.walk_payload_bags():
            if isinstance(value, dict) and "__karton_resource__" in value:
                payload_bag[key] = remote_resource_from_dict(
                    value["__karton_resource__"], minio
                )

    @staticmethod
    def unserialize(data, minio=None):
        """
        Unserialize Task instance from JSON string

        :param data: JSON-serialized task
        :type data: str or bytes
        :param minio: Minio instance (to be bound to RemoteResource objects)
        :type minio: minio.Minio, optional if you don't want to operate on them (e.g. karton-system)

        :meta private:
        """
        if not isinstance(data, str):
            data = data.decode("utf8")

        data = json.loads(data)

        task = Task(data["headers"])
        task.uid = data["uid"]
        task.root_uid = data["root_uid"]
        task.parent_uid = data["parent_uid"]
        task.status = data["status"]
        # Backwards compatibility, remove these .get's after upgrade
        task.priority = data.get("priority", TaskPriority.NORMAL)
        task.last_update = data.get("last_update", None)
        task.payload = data["payload"]
        task.payload_persistent = data["payload_persistent"]
        task.unserialize_resources(minio)
        return task

    def __repr__(self):
        return self.serialize()

    def is_asynchronic(self):
        """
        Checks whether task is asynchronic

        .. note::

            This is experimental feature.
            Read about asynchronic tasks in Advanced concepts.


        :rtype: bool
        :return: if current task is asynchronic
        """
        return self.asynchronic

    def make_asynchronic(self):
        """
        Task declares that work will be done by some remote
        handler, so task shouldn't be considered finished when process() returns

        .. note::

            This is experimental feature.
            Read about asynchronic tasks in Advanced concepts.
        """
        self.asynchronic = True

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
        Add resource to task.

        Alias for :py:meth:`add_resource`

        .. deprecated:: 3.0.0
           Use :meth:`add_payload` instead.

        :type name: str
        :param name: name of the resource
        :type resource: :py:class:`karton.ResourceBase`
        :param resource: resource to be added
        :type persistent: bool
        :param persistent: flag if the param should be persistent
        """
        self.add_payload(name, resource, persistent)

    def get_payload(self, name, default=None):
        """
        Get payload from task

        :type name: str
        :param name: name of the payload
        :type default: Any (optional)
        :param default: value to be returned if payload is not present
        :return: payload content
        """
        if name in self.payload_persistent:
            return self.payload_persistent[name]
        return self.payload.get(name, default)

    def get_resource(self, name):
        """
        Get resource from task.

        Ensures that payload contains an Resource object.
        If not - raises :class:`TypeError`

        :param name: name of the resource
        :type name: str
        :return: :py:class:`karton.ResourceBase` - resource with given name
        """
        resource = self.get_payload(name)
        if not isinstance(resource, ResourceBase):
            raise TypeError("Resource was expected but not found")
        return resource

    def remove_payload(self, name):
        """
        Removes payload for the task

        If payload doesn't exist or is persistent - raises KeyError

        :param name: payload name to be removed
        :type name: str
        """
        del self.payload[name]

    def has_payload(self, name):
        """
        Checks whether payload exists

        :param name: name of the payload to be checked
        :type name: str
        :rtype: bool
        :return: if task's payload contains payload with given name
        """
        return name in self.payload or name in self.payload_persistent

    def is_payload_persistent(self, name):
        """
        Checks whether payload exists and is persistent

        :param name: name of the payload to be checked
        :type name: str
        :rtype: bool
        :return: if task's payload with given name is persistent
        """
        return name in self.payload_persistent
