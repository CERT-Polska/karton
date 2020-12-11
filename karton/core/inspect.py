from collections import defaultdict
from typing import List

from .backend import KartonBackend, KartonBind
from .task import Task, TaskState


class KartonQueue:
    def __init__(self, bind: KartonBind, tasks: List[Task]) -> None:
        self.bind = bind
        self.tasks = tasks

    @property
    def last_update(self):
        return max([task.last_update for task in self.tasks])

    @property
    def online_consumers_count(self):
        return

    @property
    def pending_tasks(self):
        return [task for task in self.tasks if task.status != TaskState.CRASHED]

    @property
    def crashed_tasks(self):
        return [task for task in self.tasks if task.status == TaskState.CRASHED]


class KartonAnalysis:
    def __init__(self, rootid: str, tasks: List[Task]) -> None:
        self.rootid = rootid
        self.tasks = tasks

    @property
    def last_update(self):
        return max([task.last_update for task in self.tasks])

    @property
    def is_done(self):
        return len(self.pending_tasks) == 0

    @property
    def pending_tasks(self):
        return [task for task in self.tasks if task.status != TaskState.CRASHED]

    @property
    def pending_queues(self):
        return

    @property
    def crashed_tasks(self):
        return [task for task in self.tasks if task.status == TaskState.CRASHED]


class KartonState:
    """
    Karton state inspection class. Allows to make a detailed inspection
    of the pipeline and analyses state.

    .. versionadded: 4.0.0

    """
    def __init__(self, backend: KartonBackend):
        self.backend = backend
        self.binds = {
            bind.identity: bind
            for bind in backend.get_binds()
        }
        self.replicas = backend.get_online_consumers()
        self.tasks = backend.get_all_tasks()
        self.pending_tasks = [
            task for task in self.tasks
            if task.status != TaskState.FINISHED
        ]
        self.log_queue_length = backend.get_log_queue_length()

        # Tasks grouped by root_uid
        tasks_per_analysis = defaultdict(list)
        # Tasks grouped by target queue name
        tasks_per_queue = defaultdict(list)
        # Queues with available binds

        for task in self.pending_tasks:
            tasks_per_analysis[task.root_uid].append(task)
            if "receiver" not in task.headers:
                continue

            queue_name = task.headers["receiver"]
            if queue_name not in self.binds:
                # Tasks without receiver are Declared and waiting for routing
                continue

            tasks_per_queue[queue_name].append(task)

        self.analyses = {
            rootid: KartonAnalysis(rootid=rootid, tasks=tasks)
            for rootid, tasks in tasks_per_analysis.items()
        }
        self.queues = {
            queue_name: KartonQueue(bind=self.binds[queue_name], tasks=tasks)
            for queue_name, tasks in tasks_per_queue.items()
        }
