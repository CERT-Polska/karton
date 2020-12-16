from collections import defaultdict
from typing import Dict, List

from .backend import KartonBackend, KartonBind
from .task import Task, TaskState


class KartonQueue:
    def __init__(
        self, bind: KartonBind, tasks: List[Task], state: "KartonState"
    ) -> None:
        self.bind = bind
        self.tasks = tasks
        self.state = state

    @property
    def last_update(self) -> float:
        return max(task.last_update for task in self.tasks)

    @property
    def online_consumers_count(self) -> int:
        return len(self.state.replicas[self.bind.identity])

    @property
    def pending_tasks(self) -> List[Task]:
        return [task for task in self.tasks if task.status != TaskState.CRASHED]

    @property
    def crashed_tasks(self) -> List[Task]:
        return [task for task in self.tasks if task.status == TaskState.CRASHED]


class KartonAnalysis:
    def __init__(self, root_uid: str, tasks: List[Task], state: "KartonState") -> None:
        self.root_uid = root_uid
        self.tasks = tasks
        self.state = state

    @property
    def last_update(self) -> float:
        return max(task.last_update for task in self.tasks)

    @property
    def is_done(self) -> bool:
        return len(self.pending_tasks) == 0

    @property
    def pending_tasks(self) -> List[Task]:
        return [task for task in self.tasks if task.status != TaskState.CRASHED]

    @property
    def pending_queues(self) -> Dict[str, KartonQueue]:
        return get_queues_for_tasks(self.tasks, self.state)

    @property
    def crashed_tasks(self) -> List[Task]:
        return [task for task in self.tasks if task.status == TaskState.CRASHED]


def get_queues_for_tasks(
    tasks: List[Task], state: "KartonState"
) -> Dict[str, KartonQueue]:
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

    """

    def __init__(self, backend: KartonBackend) -> None:
        self.backend = backend
        self.binds = {bind.identity: bind for bind in backend.get_binds()}
        self.replicas = backend.get_online_consumers()
        self.tasks = backend.get_all_tasks()
        self.pending_tasks = [
            task for task in self.tasks if task.status != TaskState.FINISHED
        ]

        # Tasks grouped by root_uid
        tasks_per_analysis = defaultdict(list)

        for task in self.pending_tasks:
            tasks_per_analysis[task.root_uid].append(task)

        self.analyses = {
            root_uid: KartonAnalysis(root_uid=root_uid, tasks=tasks, state=self)
            for root_uid, tasks in tasks_per_analysis.items()
        }
        queues = get_queues_for_tasks(self.pending_tasks, self)
        # Present registered queues without tasks
        for bind_name, bind in self.binds.items():
            if bind_name not in queues:
                queues[bind_name] = KartonQueue(
                    bind=self.binds[bind_name], tasks=[], state=self
                )
        self.queues = queues
