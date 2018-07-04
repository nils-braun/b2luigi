import os
import sys
import time

import luigi
import luigi.interface

import subprocess


class SendJobWorker(luigi.worker.Worker):
    def _handle_next_task(self):
        # Update out own list of "running" tasks
        task_list = self._scheduler.task_list()
        for key, task in task_list.items():
            if task["status"] not in ["PENDING", "RUNNING"] and key in self._running_tasks:
                del self._running_tasks[key]
                sched_task = self._scheduled_tasks.get(key)
                self._add_task_history.append((sched_task, task["status"], True))

        time.sleep(1)

    def _run_task(self, task_id):
        if task_id in self._running_tasks:
            next(self._sleeper())
            return

        # TODO: Depend on job whether to send this to the queue or not
        task = self._scheduled_tasks[task_id]

        # TODO: use this information
        self._running_tasks[task_id] = "task"

        print("Scheduling new job", task_id)
        subprocess.call(["bsub", "-env all",
                         "python3", os.path.realpath(sys.argv[0]), "--batch-runner",
                          "--scheduler-url", self._scheduler._url,
                          "--worker-id", self._id, "--task-id", task_id],
                         )


class SendJobWorkerSchedulerFactory(luigi.interface._WorkerSchedulerFactory):
    def create_worker(self, scheduler, worker_processes, assistant=False):
        return SendJobWorker(scheduler=scheduler, assistant=assistant)