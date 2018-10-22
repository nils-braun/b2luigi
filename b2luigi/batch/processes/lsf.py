import json
import re
import subprocess

from b2luigi.batch.processes import BatchProcess, JobStatus
from b2luigi.core.utils import get_log_file_dir

from cachetools import TTLCache


class BatchJobStatusCache(TTLCache):
    def __init__(self):
        super(BatchJobStatusCache, self).__init__(maxsize=1000, ttl=20)

    def _ask_for_job_status(self, job_id=None):
        if job_id:
            output = subprocess.check_output(["bjobs", "-json", "-o", "jobid stat", str(job_id)])
        else:
            output = subprocess.check_output(["bjobs", "-json", "-o", "jobid stat"])
        output = output.decode()
        output = json.loads(output)["RECORDS"]

        for record in output:
            self[record["JOBID"]] = record["STAT"]

    def __missing__(self, job_id):
        # First, ask for all jobs
        self._ask_for_job_status(job_id=None)
        if job_id in self:
            return self[job_id]

        # Then, ask specifically for this job
        self._ask_for_job_status(job_id=job_id)
        if job_id in self:
            return self[job_id]

        raise KeyError


_batch_job_status_cache = BatchJobStatusCache()


class LSFProcess(BatchProcess):
    """
    Reference implementation of the batch process for an LSF batch system.

    We assume that the batch system shares a file system with the submission node you
    are currently working on (or at least the current folder is also available there with the same path).
    We also assume that we can run the same python interpreter there by just copying
    the current environment and calling it from the same path.
    Both requirements are fulfilled by a "normal" LSF setup, so you do not keep those in mind typically.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._batch_job_id = None

    def get_job_status(self):
        assert self._batch_job_id

        try:
            job_status = _batch_job_status_cache[self._batch_job_id]
        except KeyError:
            return JobStatus.aborted

        if job_status == "DONE":
            return JobStatus.successful
        elif job_status == "EXIT":
            return JobStatus.aborted

        return JobStatus.running

    def start_job(self):
        prefix = ["bsub", "-env all"]

        try:
            prefix += ["-q", self.task.queue]
        except AttributeError:
            pass

        log_file_dir = get_log_file_dir(self.task)
        stderr_log_file = log_file_dir + "stderr"
        stdout_log_file = log_file_dir + "stdout"

        prefix += ["-eo", stderr_log_file, "-oo", stdout_log_file]

        output = subprocess.check_output(prefix + self.task_cmd)
        output = output.decode()

        # Output of the form Job <72065926> is submitted to default queue <s>.
        match = re.search(r"<[0-9]+>", output)
        if not match:
            raise RuntimeError("Batch submission failed with output " + output)

        self._batch_job_id = match.group(0)[1:-1]

    def kill_job(self):
        if not self._batch_job_id:
            return

        subprocess.check_call(["bkill", self._batch_job_id], stdout=subprocess.DEVNULL)