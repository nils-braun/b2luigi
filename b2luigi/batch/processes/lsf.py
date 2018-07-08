import json
import re
import subprocess

from b2luigi.batch.processes import BatchProcess


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

        print("Started batch job with id", self._batch_job_id)

    def terminate(self):
        if not self._batch_job_id:
            return

        subprocess.check_call(["bkill", self._batch_job_id], stdout=subprocess.DEVNULL)