import os
import sys

import luigi
import luigi.interface

import subprocess


class SendJobWorker(luigi.worker.Worker):
    def _handle_next_task(self):
        # Update out own list of "running" tasks
        for key, task in self._scheduler.task_list().items():
            if task["status"] not in ["PENDING", "RUNNING"] and key in self._running_tasks:
                del self._running_tasks[key]
                sched_task = self._scheduled_tasks.get(key)
                self._add_task_history.append((sched_task, task["status"], True))

    def _run_task(self, task_id):
        if task_id in self._running_tasks:
            next(self._sleeper())
            return

        # TODO: Depend on job whether to send this to the queue or not
        task = self._scheduled_tasks[task_id]

        # TODO: use this information
        self._running_tasks[task_id] = "task"

        print("Scheduling new job", task_id)
        subprocess.call(["bsub", "-env all", "python3", os.path.realpath(sys.argv[0]), "--batch-runner",
                          "--scheduler-url", self._scheduler._url,
                          "--worker-id", self._id, "--task-id", task_id],
                         )


class SendJobWorkerSchedulerFactory(luigi.interface._WorkerSchedulerFactory):
    def create_worker(self, scheduler, worker_processes, assistant=False):
        return SendJobWorker(scheduler=scheduler, assistant=assistant)


class OneTimeRemoteScheduler(luigi.rpc.RemoteScheduler):
    def add_task(self, **kwargs):
        # We do not want to send a pending message to the central scheduler, as this would override already given
        # information by other workers
        if kwargs["status"] == "PENDING":
            return
        super().add_task(**kwargs)


class OneTimeWorker(luigi.worker.Worker):
    def __init__(self, task_id, **kwargs):
        super().__init__(**kwargs)
        self._task_id = task_id

    def run(self):
        # The run is quite simple, as we only want to do a single task
        # TODO: just doing the task should be made much simpler!
        # TODO: I actually just need to tell the central scheduler in the end,
        # that we are done!
        self._add_worker()
        self._run_task(self._task_id)
        self._handle_next_task()

        return True


class OneTimeWorkerSchedulerFactory(luigi.interface._WorkerSchedulerFactory):
    def __init__(self, task_id, worker_id):
        self.task_id = task_id
        self.worker_id = worker_id

        super().__init__()

    def create_remote_scheduler(self, url):
        return OneTimeRemoteScheduler(url)

    def create_worker(self, scheduler, worker_processes, assistant=False):
        return OneTimeWorker(worker_id=self.worker_id, task_id=self.task_id,
                             scheduler=scheduler, worker_processes=worker_processes, assistant=assistant)
