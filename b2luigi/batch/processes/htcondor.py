import json
import re
import subprocess
import enum


from b2luigi.batch.processes import BatchProcess, JobStatus, BatchJobStatusCache
from b2luigi.core.utils import get_log_file_dir

from cachetools import TTLCache


class HTCondorJobStatusCache(BatchJobStatusCache):

    def __init__(self):
        super(HTCondorJobStatusCache, self).__init__()

    def _ask_for_job_status(self, job_id=None):
        cmd = ["condor_q", "-json", "-attributes", "ClusterId,ProcId,JobStatus"]

        if job_id:
            cmd.append(str(job_id))
            output = subprocess.check_output(cmd)
        else:
            output = subprocess.check_output(cmd)

        output = output.decode()
        dict_list = [json.loads(status_dict) for status_dict
                     in output.lstrip("[\n").rstrip("\n]\n").split("\n,\n")]

        for status_dict in dict_list:
            self[status_dict["ClusterId"]] = status_dict["JobStatus"]


class HTCondorJobStatus(enum.Enum):
    idle = 1
    running = 2
    removed = 3
    completed = 4
    held = 5
    submission_err = 6


class HTCondorProcess(BatchProcess):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        pass

    def get_job_status(self):
        pass

    def start_job(self):
        pass

    def kill_job(self):
        pass
