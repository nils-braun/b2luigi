import subprocess

from b2luigi.batch.processes import BatchProcess, JobStatus
from b2luigi.core.utils import get_log_file_dir
from b2luigi.core.executable import create_executable_wrapper


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
        log_file_dir = get_log_file_dir(self.task)
        stdout_log_file = log_file_dir + "stdout"
        stderr_log_file = log_file_dir + "stderr"

        executable_file = create_executable_wrapper(self.task)

        with open(stdout_log_file, "w") as stdout_file:
            with open(stderr_log_file, "w") as stderr_file:
                self._process = subprocess.Popen([executable_file], stdout=stdout_file, stderr=stderr_file)

    def kill_job(self):
        if not self._process:
            return

        self._process.kill()

    def get_job_output(self):
        return ""
