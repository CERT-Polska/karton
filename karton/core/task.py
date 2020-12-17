import enum
import json
import time
import uuid
import warnings
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Tuple, Union

from .resource import RemoteResource, ResourceBase

if TYPE_CHECKING:
    from .backend import KartonBackend


class TaskState(enum.Enum):
    DECLARED = "Declared"  # Task declared in TASKS_QUEUE
    SPAWNED = "Spawned"  # Task spawned into subsystem queue
    STARTED = "Started"  # Task is running in subsystem
    FINISHED = "Finished"  # Task finished (ready to forget)
    CRASHED = "Crashed"  # Task crashed


class TaskPriority(enum.Enum):
    HIGH = "high"
    NORMAL = "normal"
    LOW = "low"


class Task(object):
    """
    Task representation with headers and resources.

    :param headers: Routing information for other systems, this is what allows for \
                    evaluation of given system usefulness for given task. \
                    Systems filter by these.
    :param payload: Any instance of :py:class:`dict` - contains resources \
                    and additional informations
    :param payload_persistent: Persistent payload set for whole task subtree, \
                               propagated from initial task
    :param priority: Priority of whole task subtree, \
                     propagated from initial task like `payload_persistent`
    :param parent_uid: Id of a routed task that has created this task by a karton with \
                       `py:meth:`send_task
    :param root_uid: Id of a unrouted task that is the root of this tasks analysis tree
    :param orig_uid: Id of a unrouted (or crashed routed) task that was forked to \
                     create this task
    :param uid: This tasks unique identifier
    :param error: Traceback of a exception that happened while performing this task
    """

    def __init__(
        self,
        headers: Dict[str, Any],
        payload: Optional[Dict[str, Any]] = None,
        payload_persistent: Optional[Dict[str, Any]] = None,
        priority: Optional[TaskPriority] = None,
        parent_uid: Optional[str] = None,
        root_uid: Optional[str] = None,
        orig_uid: Optional[str] = None,
        uid: Optional[str] = None,
        error: Optional[List[str]] = None,
    ) -> None:
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

        self.orig_uid = orig_uid
        self.parent_uid = parent_uid

        self.error = error
        self.headers = headers
        self.status = TaskState.DECLARED

        self.last_update: float = time.time()
        self.priority = priority or TaskPriority.NORMAL

        self.payload = dict(payload)
        self.payload_persistent = dict(payload_persistent)

    def fork_task(self) -> "Task":
        """
        Fork task to transfer single task to many queues (but use different UID).

        Used internally by karton-system

        :return: Forked copy of the original task

        :meta private:
        """
        new_task = Task(
            headers=self.headers,
            payload=self.payload,
            payload_persistent=self.payload_persistent,
            priority=self.priority,
            parent_uid=self.parent_uid,
            root_uid=self.root_uid,
            orig_uid=self.uid,
        )
        return new_task

    def derive_task(self, headers: Dict[str, Any]) -> "Task":
        """
        Creates copy of task with different headers,
        useful for proxying resource with added metadata.

        .. code-block:: python

            class MZClassifier(Karton):
                identity = "karton.mz-classifier"
                filters = {
                    "type": "sample",
                    "kind": "raw"
                }

                def process(self, task: Task) -> None:
                    sample = task.get_resource("sample")
                    if sample.content.startswith(b"MZ"):
                        self.log.info("MZ detected!")
                        task = task.derive_task({
                            "type": "sample",
                            "kind": "exe"
                        })
                        self.send_task(task)
                    self.log.info("Not a MZ :<")

        .. versionchanged:: 3.0.0

            Moved from static method to regular method:

            :code:`Task.derive_task(headers, task)` must be
            ported to :code:`task.derive_task(headers)`

        :param headers: New headers for the task
        :return: Copy of task with new headers
        """
        new_task = Task(
            headers=headers,
            payload=self.payload,
            payload_persistent=self.payload_persistent,
        )
        return new_task

    def matches_filters(self, filters: List[Dict[str, Any]]) -> bool:
        """
        Checks whether provided task headers match filters

        :param filters: Task header filters
        :return: True if task headers match specific filters

        :meta private:
        """
        return any(
            # If any of consumer filters matches the header
            all(
                # Match: all consumer filter fields match the task header
                self.headers.get(bind_key) == bind_value
                for bind_key, bind_value in task_filter.items()
            )
            for task_filter in filters
        )

    def set_task_parent(self, parent: "Task"):
        """
        Bind existing Task to parent task

        :param parent: Task to bind to

        :meta private:
        """
        self.parent_uid = parent.uid
        self.root_uid = parent.root_uid

    def merge_persistent_payload(self, other_task: "Task") -> None:
        """
        Merge persistent payload from another task

        :param other_task: Task from which to merge persistent payload

        :meta private:
        """
        for name, content in other_task.payload_persistent.items():
            self.payload_persistent[name] = content
            if name in self.payload:
                # Delete conflicting non-persistent payload
                del self.payload[name]

    def serialize(self, indent: Optional[int] = None) -> str:
        """
        Serialize task data into JSON string
        :param indent: Indent to use while serializing
        :return: Serialized task data

        :meta private:
        """

        class KartonResourceEncoder(json.JSONEncoder):
            def default(kself, obj: Any):
                if isinstance(obj, ResourceBase):
                    return {"__karton_resource__": obj.to_dict()}
                return json.JSONEncoder.default(kself, obj)

        return json.dumps(
            {
                "uid": self.uid,
                "root_uid": self.root_uid,
                "parent_uid": self.parent_uid,
                "orig_uid": self.orig_uid,
                "status": self.status.value,
                "priority": self.priority.value,
                "last_update": self.last_update,
                "payload": self.payload,
                "payload_persistent": self.payload_persistent,
                "headers": self.headers,
                "error": self.error,
            },
            cls=KartonResourceEncoder,
            indent=indent,
            sort_keys=True,
        )

    def walk_payload_bags(self) -> Iterator[Tuple[Dict[str, Any], str, Any]]:
        """
        Iterate over all payload bags and payloads contained in them

        Generates tuples (payload_bag, key, value)

        :return: An iterator over all task payload bags
        """
        for payload_bag in [self.payload, self.payload_persistent]:
            for key, value in payload_bag.items():
                yield payload_bag, key, value

    def iterate_resources(self) -> Iterator[Tuple[str, ResourceBase]]:
        """
        Get list of resource objects bound to Task

        Generates tuples (key, value)

        :return: An iterator over all task resources
        """
        for _, key, value in self.walk_payload_bags():
            if isinstance(value, ResourceBase):
                yield key, value

    def unserialize_resources(self, backend: Optional["KartonBackend"]) -> None:
        """
        Transforms __karton_resource__ serialized entries into
        RemoteResource object instances

        :param backend: KartonBackend to use while unserializing resources

        :meta private:
        """
        for payload_bag, key, value in self.walk_payload_bags():
            if isinstance(value, dict) and "__karton_resource__" in value:
                payload_bag[key] = RemoteResource.from_dict(
                    value["__karton_resource__"], backend
                )

    @staticmethod
    def unserialize(
        data: Union[str, bytes], backend: Optional["KartonBackend"] = None
    ) -> "Task":
        """
        Unserialize Task instance from JSON string

        :param data: JSON-serialized task
        :param backend: Backend instance to be bound to RemoteResource objects
        :return: Unserialized Task object

        :meta private:
        """
        if not isinstance(data, str):
            data = data.decode("utf8")

        task_data = json.loads(data)

        task = Task(task_data["headers"])
        task.uid = task_data["uid"]
        task.root_uid = task_data["root_uid"]
        task.parent_uid = task_data["parent_uid"]
        # Compatibility with <= 3.x.x (get)
        task.orig_uid = task_data.get("orig_uid", None)
        task.status = TaskState(task_data["status"])
        # Compatibility with <= 3.x.x (get)
        task.error = task_data.get("error")
        # Compatibility with <= 2.x.x (get)
        task.priority = (
            TaskPriority(task_data.get("priority"))
            if "priority" in task_data
            else TaskPriority.NORMAL
        )
        task.last_update = task_data.get("last_update", None)
        task.payload = task_data["payload"]
        task.payload_persistent = task_data["payload_persistent"]
        task.unserialize_resources(backend)
        return task

    def __repr__(self) -> str:
        return self.serialize()

    def add_payload(self, name: str, content: Any, persistent: bool = False) -> None:
        """
        Add payload to task

        :param name: Name of the payload
        :param content: Payload to be added
        :param persistent: Flag if the payload should be persistent
        """
        if name in self.payload:
            raise ValueError("Payload already exists")

        if name in self.payload_persistent:
            raise ValueError("Payload already exists in persistent payloads")

        if not persistent:
            self.payload[name] = content
        else:
            self.payload_persistent[name] = content

    def add_resource(
        self, name: str, resource: ResourceBase, persistent: bool = False
    ) -> None:
        """
        Add resource to task.

        Alias for :py:meth:`add_payload`

        .. deprecated:: 3.0.0
           Use :meth:`add_payload` instead.

        :param name: Name of the resource
        :param resource: Resource to be added
        :param persistent: Flag if the resource should be persistent
        """
        warnings.warn(
            "add_resource is deprecated, use add_payload instead",
            DeprecationWarning,
            stacklevel=2,
        )
        self.add_payload(name, resource, persistent)

    def get_payload(self, name: str, default: Any = None) -> Any:
        """
        Get payload from task

        :param name: name of the payload
        :param default: Value to be returned if payload is not present
        :return: Payload content
        """
        if name in self.payload_persistent:
            return self.payload_persistent[name]
        return self.payload.get(name, default)

    def get_resource(self, name: str) -> ResourceBase:
        """
        Get resource from task.

        Ensures that payload contains an Resource object.
        If not - raises :class:`TypeError`

        :param name: Name of the resource to get
        :return: :py:class:`karton.ResourceBase` - resource with given name
        """
        resource = self.get_payload(name)
        if not isinstance(resource, ResourceBase):
            raise TypeError("Resource was expected but not found")
        return resource

    def remove_payload(self, name: str) -> None:
        """
        Removes payload for the task

        If payload doesn't exist or is persistent - raises KeyError

        :param name: Payload name to be removed
        """
        del self.payload[name]

    def has_payload(self, name: str) -> bool:
        """
        Checks whether payload exists

        :param name: Name of the payload to be checked
        :return: If tasks payload contains a value with given name
        """
        return name in self.payload or name in self.payload_persistent

    def is_payload_persistent(self, name: str) -> bool:
        """
        Checks whether payload exists and is persistent

        :param name: Name of the payload to be checked
        :return: If tasks payload with given name is persistent
        """
        return name in self.payload_persistent
