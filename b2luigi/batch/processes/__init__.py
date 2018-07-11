import os
import sys
import time

import enum

import luigi
import luigi.scheduler


class JobStatus(enum.Enum):
    running = "running"
    successful = "successful"
    aborted = "aborted"


class BatchProcess:
    def __init__(self, task, scheduler, result_queue, worker_timeout):
        self.use_multiprocessing = False
        self.task = task
        self.timeout_time = time.time() + worker_timeout if worker_timeout else None

        self._result_queue = result_queue
        self._scheduler = scheduler

        self.task_cmd = [sys.executable, os.path.realpath(sys.argv[0]),
                         "--batch-runner",
                         "--task-id", task.task_id]

    @property
    def exitcode(self):
        # We cheat here a bit: if the exit code is set to 0 all the time, we can always use the result queue for
        # delivering the result
        return 0

    def get_job_status(self):
        raise NotImplementedError

    def start_job(self):
        raise NotImplementedError

    def kill_job(self):
        raise NotImplementedError

    def get_job_output(self):
        raise NotImplementedError

    def run(self):
        self.start_job()

    def terminate(self):
        self.kill_job()

    def is_alive(self):
        job_status = self.get_job_status()

        if job_status == JobStatus.successful:
            job_output = self.get_job_output()
            self._put_to_result_queue(status=luigi.scheduler.DONE, explanation=job_output)
            return False
        elif job_status == JobStatus.aborted:
            job_output = self.get_job_output()
            self._put_to_result_queue(status=luigi.scheduler.FAILED, explanation=job_output)
            return False
        elif job_status == JobStatus.running:
            return True

        raise ValueError("get_job_status() returned an unknown job state!")

    def _put_to_result_queue(self, status, explanation):
        missing = []
        new_deps = []
        self._result_queue.put((self.task.task_id, status, explanation, missing, new_deps))
