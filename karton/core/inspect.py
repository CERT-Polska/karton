from collections import defaultdict
from typing import Dict, List, Optional

from .backend import KartonBackend, KartonBind
from .task import Task, TaskState


class KartonQueue:
    """
    View object representing a Karton queue

    :param bind: :class:`KartonBind` object representing the queue bind
    :param tasks: List of tasks currently in queue
    :param state: :class:`KartonState` object to be used
    """

    def __init__(
        self, bind: KartonBind, tasks: List[Task], state: "KartonState"
    ) -> None:
        self.bind = bind
        self.tasks = tasks
        self.state = state

    @property
    def last_update(self) -> float:
        """Get the last task update from this queue"""
        return max(task.last_update for task in self.tasks)

    @property
    def online_consumers_count(self) -> int:
        """Get number of consumers listening on this queue"""
        return len(self.state.replicas[self.bind.identity])

    @property
    def pending_tasks(self) -> List[Task]:
        """Get queue pending tasks"""
        return [task for task in self.tasks if task.status != TaskState.CRASHED]

    @property
    def crashed_tasks(self) -> List[Task]:
        """Get queue crashed tasks"""
        return [task for task in self.tasks if task.status == TaskState.CRASHED]


class KartonAnalysis:
    """
    View object representing a Karton task analysis

    :param root_uid: Analysis root task uid
    :param tasks: List of tasks
    :param state: :class:`KartonState` object to be used
    """

    def __init__(self, root_uid: str, tasks: List[Task], state: "KartonState") -> None:
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
    def pending_tasks(self) -> List[Task]:
        """Get analysis pending tasks"""
        return [task for task in self.tasks if task.status != TaskState.CRASHED]

    @property
    def pending_queues(self) -> Dict[str, KartonQueue]:
        """Group analysis tasks by their queues"""
        return get_queues_for_tasks(self.tasks, self.state)

    @property
    def crashed_tasks(self) -> List[Task]:
        """Get analysis crashed tasks"""
        return [task for task in self.tasks if task.status == TaskState.CRASHED]


def get_queues_for_tasks(
    tasks: List[Task], state: "KartonState"
) -> Dict[str, KartonQueue]:
    """
    Group task objects by their queue name

    :param tasks: Task objects to group
    :param state: :class:`KartonState` object to be used
    :return: A dictionary containing the queue names and lists of tasks
    """
    tasks_per_queue = defaultdict(list)

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
        queue_name: KartonQueue(bind=state.binds[queue_name], tasks=tasks, state=state)
        for queue_name, tasks in tasks_per_queue.items()
    }


class KartonState:
    """
    Karton state inspection class. Allows to make a detailed inspection
    of the pipeline and analyses state.

    .. versionadded: 4.0.0

    :param backend: :py:meth:`KartonBackend` object to use for data fetching
    """

    def __init__(self, backend: KartonBackend, parse_resources: bool = False) -> None:
        self.backend = backend
        self.binds = {bind.identity: bind for bind in backend.get_binds()}
        self.replicas = backend.get_online_consumers()
        self.parse_resources = parse_resources

        self._tasks: Optional[List[Task]] = None
        self._pending_tasks: Optional[List[Task]] = None
        self._analyses: Optional[Dict[str, KartonAnalysis]] = None
        self._queues: Optional[Dict[str, KartonQueue]] = None

    @property
    def tasks(self) -> List[Task]:
        if self._tasks is None:
            self._tasks = self.backend.get_all_tasks(
                parse_resources=self.parse_resources
            )
        return self._tasks

    @property
    def pending_tasks(self) -> List[Task]:
        if self._pending_tasks is None:
            self._pending_tasks = [
                task for task in self.tasks if task.status != TaskState.FINISHED
            ]
        return self._pending_tasks

    @property
    def analyses(self) -> Dict[str, KartonAnalysis]:
        if self._analyses is None:
            # Tasks grouped by root_uid
            tasks_per_analysis = defaultdict(list)

            for task in self.pending_tasks:
                tasks_per_analysis[task.root_uid].append(task)

            self._analyses = {
                root_uid: KartonAnalysis(root_uid=root_uid, tasks=tasks, state=self)
                for root_uid, tasks in tasks_per_analysis.items()
            }
        return self._analyses

    @property
    def queues(self) -> Dict[str, KartonQueue]:
        if self._queues is None:
            queues = get_queues_for_tasks(self.pending_tasks, self)
            # Present registered queues without tasks
            for bind_name, bind in self.binds.items():
                if bind_name not in queues:
                    queues[bind_name] = KartonQueue(
                        bind=self.binds[bind_name], tasks=[], state=self
                    )
            self._queues = queues
        return self._queues

    def get_analysis(self, root_uid: str) -> KartonAnalysis:
        return KartonAnalysis(
            root_uid=root_uid,
            tasks=list(
                self.backend.iter_task_tree(
                    root_uid, parse_resources=self.parse_resources
                )
            ),
            state=self,
        )
