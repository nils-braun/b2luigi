from collections import Counter, defaultdict
from multiprocessing import Queue
from typing import Dict

import luigi
from b2luigi.batch.processes import JobStatus
from rich.live import Live
from rich.table import Table

status_queue = Queue()


def _set_task_status(queue: Queue, task: luigi.task, status: str) -> None:
    queue.put({"task_id": task.task_id, "task_family": task.get_task_family(), "status": status})


@luigi.Task.event_handler(luigi.Event.START)
def start_handler(task):
    # Process(target=_set_task_status, args=(status_queue, task, "Started",))
    _set_task_status(status_queue, task, JobStatus.running)


@luigi.Task.event_handler(luigi.Event.SUCCESS)
def success_handler(task):
    _set_task_status(status_queue, task, JobStatus.successful)


@luigi.Task.event_handler(luigi.Event.FAILURE)
def failure_handler(task):
    """Will be called directly after a failed execution
       of `run` on any JobTask subclass
    """
    _set_task_status(status_queue, task, JobStatus.aborted)


@luigi.Task.event_handler(luigi.Event.PROCESS_FAILURE)
def process_failure_handler(task):
    _set_task_status(status_queue, task, JobStatus.aborted)


def update_task_statuses(queue: Queue, task_statuses: Dict[str, str]) -> Dict[str, str]:
    task_status = queue.get()
    task_statuses[task_status["task_id"]] = {"status": task_status["status"], "task_family": task_status["task_family"]}


def _count_statuses_by_task_family(task_statuses: Dict[str, str]) -> Dict[str, Counter]:
    n_statuses_by_task_family = defaultdict(Counter)
    for task_status in task_statuses.values():
        n_statuses_by_task_family[task_status["task_family"]][task_status["status"]] += 1
    return n_statuses_by_task_family


def generate_task_table(task_statuses: Dict[str, Dict]) -> Table:
    n_statuses_by_task_family = _count_statuses_by_task_family(task_statuses)

    table = Table(title="Task Overview")
    table.add_column("Task Family")
    table.add_column("Running")
    table.add_column("Successful")
    table.add_column("Failed")

    for task_family, status_counts in n_statuses_by_task_family.items():
        status_counts = [
            str(status_counts[job_status]) for job_status in (JobStatus.running, JobStatus.successful, JobStatus.aborted)
        ]
        table.add_row(task_family, *status_counts)
    return table


def show_task_overview():
    task_statuses = {}
    with Live(generate_task_table(task_statuses), screen=True) as live:
        try:
            while True:
                update_task_statuses(status_queue, task_statuses)
                live.update(generate_task_table(task_statuses))
        except KeyboardInterrupt:
            return
