import json
import re
import subprocess

from b2luigi.batch.processes import BatchProcess, JobStatus
from b2luigi.core.utils import get_log_file_dir


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

        output = subprocess.check_output(["bjobs", "-json", "-o", "stat", self._batch_job_id])
        output = output.decode()
        output = json.loads(output)["RECORDS"][0]

        if "STAT" not in output:
            return JobStatus.aborted

        job_status = output["STAT"]

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