from collections import defaultdict

from .backend import KartonBackendProtocol, KartonBind
from .task import Task, TaskState


class KartonQueue:
    """
    View object representing a Karton queue.

    :param summary: :class:`QueueSummary` with queue state data
    :param state: :class:`KartonState` object to be used for lazy loading
    """

    def __init__(
        self,
        bind: KartonBind,
        replicas: int,
        state: "KartonState",
        tasks: list[Task] | None = None,
        pending_task_uids: list[str] | None = None,
        crashed_task_uids: list[str] | None = None,
    ) -> None:
        self.bind = bind
        self.replicas = replicas
        self.state = state
        self.pending_task_uids: list[str] = pending_task_uids or []
        self.crashed_task_uids: list[str] = crashed_task_uids or []
        self._pending_tasks: list[Task] | None = None
        self._crashed_tasks: list[Task] | None = None

        if tasks is not None:
            self._pending_tasks = self._filter_pending_tasks(tasks)
            self._crashed_tasks = self._filter_crashed_tasks(tasks)
            self.pending_task_uids = [task.uid for task in self._pending_tasks]
            self.crashed_task_uids = [task.uid for task in self._crashed_tasks]

    @staticmethod
    def _filter_pending_tasks(tasks: list[Task]) -> list[Task]:
        return [
            task
            for task in tasks
            if task.status not in (TaskState.FINISHED, TaskState.CRASHED)
        ]

    @staticmethod
    def _filter_crashed_tasks(tasks: list[Task]) -> list[Task]:
        return [task for task in tasks if task.status == TaskState.CRASHED]

    @property
    def last_update(self) -> float:
        """Get the last task update from this queue"""
        return max(task.last_update for task in self.tasks)

    @property
    def online_consumers_count(self) -> int:
        """Get number of consumers listening on this queue"""
        return self.replicas

    @property
    def tasks(self) -> list[Task]:
        """
        Get all non-finished tasks in this queue (pending and crashed).

        For the direct backend, tasks are pre-loaded. For the gateway
        backend, tasks are fetched lazily on first access.
        """
        if self._pending_tasks is None and self._crashed_tasks is None:
            # Load everything in one request
            tasks = self.state.backend.get_tasks(
                self.pending_task_uids + self.crashed_task_uids
            )
            self._pending_tasks = self._filter_pending_tasks(tasks)
            self._crashed_tasks = self._filter_crashed_tasks(tasks)

        return self.pending_tasks + self.crashed_tasks

    @property
    def pending_tasks(self) -> list[Task]:
        """Get queue pending tasks"""
        if self._pending_tasks is None:
            tasks = self.state.backend.get_tasks(self.pending_task_uids)
            self._pending_tasks = self._filter_pending_tasks(tasks)
        return self._pending_tasks

    @property
    def crashed_tasks(self) -> list[Task]:
        """Get queue crashed tasks"""
        if self._crashed_tasks is None:
            tasks = self.state.backend.get_tasks(self.crashed_task_uids)
            self._crashed_tasks = self._filter_crashed_tasks(tasks)
        return self._crashed_tasks


class KartonAnalysis:
    """
    View object representing a Karton task analysis.

    :param root_uid: Analysis root task uid
    :param tasks: List of tasks (pending and crashed, excluding finished)
    :param state: :class:`KartonState` object to be used
    """

    def __init__(self, root_uid: str, tasks: list[Task], state: "KartonState") -> None:
        self.root_uid = root_uid
        self.tasks = tasks
        self.state = state

    @property
    def last_update(self) -> float:
        """Check the last task update from the analysis"""
        return max(task.last_update for task in self.tasks)

    @property
    def is_done(self) -> bool:
        """Check if the analysis is completely done"""
        return len(self.pending_tasks) == 0

    @property
    def pending_tasks(self) -> list[Task]:
        """Get analysis pending tasks"""
        return [
            task
            for task in self.tasks
            if task.status not in (TaskState.FINISHED, TaskState.CRASHED)
        ]

    @property
    def pending_queues(self) -> dict[str, "KartonQueue"]:
        """Group analysis tasks by their queues"""
        return get_queues_for_tasks(self.tasks, self.state)

    @property
    def crashed_tasks(self) -> list[Task]:
        """Get analysis crashed tasks"""
        return [task for task in self.tasks if task.status == TaskState.CRASHED]


def get_queues_for_tasks(
    tasks: list[Task], state: "KartonState"
) -> dict[str, KartonQueue]:
    """
    Group task objects by their queue name.

    :param tasks: Task objects to group
    :param state: :class:`KartonState` object to be used
    :return: A dictionary containing the queue names and :class:`KartonQueue` objects
    """
    tasks_per_queue: dict[str, list[Task]] = defaultdict(list)

    for task in tasks:
        if "receiver" not in task.headers:
            # Tasks without receiver are Declared and waiting for routing
            continue
        queue_name = task.headers["receiver"]
        if queue_name not in state.binds:
            # No known bind, dangling task for non-existent queue
            continue
        tasks_per_queue[queue_name].append(task)

    return {
        queue_name: KartonQueue(
            bind=state.binds[queue_name],
            replicas=len(state.replicas.get(queue_name, [])),
            state=state,
            tasks=tasks,
        )
        for queue_name, tasks in tasks_per_queue.items()
    }


class KartonState:
    """
    Karton state inspection class. Allows to make a detailed inspection
    of the pipeline and analyses state.

    Works with both the direct backend (Redis) and the gateway backend
    (REST API). When using the gateway backend, queue task lists are
    fetched lazily on first access to :attr:`KartonQueue.pending_tasks`
    or :attr:`KartonQueue.crashed_tasks`.

    :param backend: :py:class:`KartonBackend` object to use for data fetching
    :param parse_resources: If set to False (default), resources are not
        parsed to speed up deserialization.
    """

    def __init__(
        self, backend: KartonBackendProtocol, parse_resources: bool = False
    ) -> None:
        self.backend = backend
        self.binds = {bind.identity: bind for bind in backend.get_binds()}
        self.replicas = backend.get_online_identities()
        self.parse_resources = parse_resources

        self._queues: dict[str, KartonQueue] | None = None

    @property
    def queues(self) -> dict[str, KartonQueue]:
        """
        Get all queues (binds) with their task state.

        Returns a dictionary mapping queue identity to
        :class:`KartonQueue`. Registered queues without any tasks
        are included with empty task lists.
        """
        if self._queues is None:
            queues_state = self.backend.get_queues(parse_resources=self.parse_resources)
            self._queues = {
                identity: KartonQueue(
                    bind=queue_state.bind,
                    replicas=queue_state.replicas,
                    state=self,
                    pending_task_uids=queue_state.pending_task_uids,
                    crashed_task_uids=queue_state.crashed_task_uids,
                )
                for identity, queue_state in queues_state.items()
            }
        return self._queues

    def get_analysis(self, root_uid: str) -> KartonAnalysis:
        """
        Get a detailed view of a single analysis (task tree).

        :param root_uid: Root identifier of the analysis task tree
        :return: :class:`KartonAnalysis` object
        """
        analysis_tasks = self.backend.get_analysis_tasks(root_uid)
        tasks = [task for task_list in analysis_tasks.values() for task in task_list]
        return KartonAnalysis(root_uid=root_uid, tasks=tasks, state=self)
