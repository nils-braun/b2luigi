import json
import os
import re
import sys
import time

import luigi
import luigi.interface

import subprocess

from b2luigi.core.settings import get_setting

import enum


class BatchSystems(enum.Enum):
    lsf = "lsf"
    htcondor = "htcondor"
    local = "local"


class BatchProcess:
    def __init__(self, result_queue, worker_timeout, task, scheduler):
        self.use_multiprocessing = False
        self.exitcode = 0
        self.task = task
        self.timeout_time = time.time() + worker_timeout if worker_timeout else None

        self._result_queue = result_queue
        self._scheduler = scheduler

        self.task_cmd = [sys.executable, os.path.realpath(sys.argv[0]),
                         "--batch-runner",
                         "--task-id", task.task_id]

    def run(self):
        raise NotImplementedError

    def is_alive(self):
        raise NotImplementedError

    def terminate(self):
        raise NotImplementedError

    def put_to_result_queue(self):
        expl = ""
        missing = []
        new_deps = []
        self._result_queue.put((self.task.task_id, luigi.scheduler.DONE, expl, missing, new_deps))


class LSFProcess(BatchProcess):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._batch_job_id = None

    def is_alive(self):
        assert self._batch_job_id

        output = subprocess.check_output(["bjobs", "-json", "-o", "stat exit_code", self._batch_job_id])
        output = output.decode()
        output = json.loads(output)["RECORDS"][0]

        if "STAT" not in output:
            self.exitcode = -1
            return False

        job_status = output["STAT"]
        self.exitcode = output["EXIT_CODE"]

        if job_status == "DONE":
            self.put_to_result_queue()
            return False
        elif job_status == "EXIT":
            return False

        return True

    def run(self):
        prefix = ["bsub", "-env all"]

        try:
            prefix += ["-q", self.task.queue]
        except AttributeError:
            pass

        # Automatic requeing?

        try:
            stdout_log_file = self.task.log_files["stdout"]
            stderr_log_file = self.task.log_files["stderr"]
            prefix += ["-eo", stderr_log_file, "-oo", stdout_log_file]
        except AttributeError:
            pass

        output = subprocess.check_output(prefix + self.task_cmd)
        output = output.decode()

        # Output of the form Job <72065926> is submitted to default queue <s>.
        match = re.search(r"<[0-9]+>", output)
        if not match:
            raise RuntimeError("Batch submission failed with output " + output)

        self._batch_job_id = match.group(0)[1:-1]

    def terminate(self):
        if not self._batch_job_id:
            return

        subprocess.check_call(["bkill", self._batch_job_id], stdout=subprocess.DEVNULL)


class SendJobWorker(luigi.worker.Worker):
    def _create_task_process(self, task):
        batch_system = get_setting("batch_system", BatchSystems.lsf)

        # TODO: also use the task to choose the batch system!

        if batch_system == BatchSystems.lsf:
            return LSFProcess(task=task, scheduler=self._scheduler,
                              result_queue=self._task_result_queue, worker_timeout=self._config.timeout)
        elif batch_system == BatchSystems.htcondor:
            raise NotImplementedError
        else:
            return super()._create_task_process(task)


class SendJobWorkerSchedulerFactory(luigi.interface._WorkerSchedulerFactory):
    def create_worker(self, scheduler, worker_processes, assistant=False):
        return SendJobWorker(scheduler=scheduler, worker_processes=worker_processes, assistant=assistant)
