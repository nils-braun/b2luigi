import json
import re
import subprocess
import os

from b2luigi.batch.processes import BatchProcess, JobStatus
from b2luigi.batch.cache import BatchJobStatusCache
from b2luigi.core.utils import get_log_file_dir
from b2luigi.core.executable import create_executable_wrapper


class LSFJobStatusCache(BatchJobStatusCache):
    def __init__(self):
        super(LSFJobStatusCache, self).__init__()

    def _ask_for_job_status(self, job_id=None):
        if job_id:
            output = subprocess.check_output(["bjobs", "-json", "-o", "jobid stat", str(job_id)])
        else:
            output = subprocess.check_output(["bjobs", "-json", "-o", "jobid stat"])
        output = output.decode()
        output = json.loads(output)["RECORDS"]

        for record in output:
            self[record["JOBID"]] = record["STAT"]


_batch_job_status_cache = LSFJobStatusCache()


class LSFProcess(BatchProcess):
    """
    Reference implementation of the batch process for a LSF batch system.

    Additional to the basic batch setup (see :ref:`batch-label`), additional 
    LSF-specific things are:

    * the LSF queue can be controlled via the ``queue`` parameter, e.g.

      .. code-block:: python

        class MyLongTask(b2luigi.Task):
            queue = "l"

      The default is the short queue "s".

    * By default, the environment variables from the scheduler are copied to
      the workers.
      This also applies we start in the same working directory and can reuse
      the same executable etc.
      Normally, you do not need to supply ``env_script`` or alike.
       
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._batch_job_id = None

    def get_job_status(self):
        if not self._batch_job_id:
            return JobStatus.aborted

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
        command = ["bsub", "-env all"]

        try:
            command += ["-q", self.task.queue]
        except AttributeError:
            pass

        log_file_dir = get_log_file_dir(self.task)
        os.makedirs(log_file_dir, exist_ok=True)

        stdout_log_file = os.path.join(log_file_dir, "stdout")
        stderr_log_file = os.path.join(log_file_dir, "stderr")

        command += ["-eo", stderr_log_file, "-oo", stdout_log_file]

        executable_file = create_executable_wrapper(self.task)
        command.append(executable_file)

        output = subprocess.check_output(command)
        output = output.decode()

        # Output of the form Job <72065926> is submitted to default queue <s>.
        match = re.search(r"<[0-9]+>", output)
        if not match:
            raise RuntimeError("Batch submission failed with output " + output)

        self._batch_job_id = match.group(0)[1:-1]

    def kill_job(self):
        if not self._batch_job_id:
            return

        subprocess.run(["bkill", self._batch_job_id], stdout=subprocess.DEVNULL)
