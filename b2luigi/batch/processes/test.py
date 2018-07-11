import subprocess

from b2luigi.batch.processes import BatchProcess, JobStatus
from b2luigi.core.utils import get_log_files


class TestProcess(BatchProcess):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._process = None

    def get_job_status(self):
        if not self._process:
            return JobStatus.aborted

        returncode = self._process.poll()

        if returncode is None:
            return JobStatus.running
        elif returncode:
            return JobStatus.aborted
        else:
            return JobStatus.successful

    def start_job(self):
        stdout_log_file, stderr_log_file = get_log_files(self.task)

        with open(stdout_log_file, "w") as stdout:
            with open(stderr_log_file, "w") as stderr:
                self._process = subprocess.Popen(self.task_cmd, stdout=stdout, stderr=stderr)

    def kill_job(self):
        if not self._process:
            return

        self._process.kill()

    def get_job_output(self):
        return ""
