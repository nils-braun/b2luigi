import json
import logging
import os
import re
import stat
import subprocess
import enum
import sys

from b2luigi.core.utils import get_setting, get_output_dirs
from b2luigi.batch.processes import BatchProcess, JobStatus, BatchJobStatusCache
from b2luigi.core.utils import get_log_file_dir


class HTCondorJobStatusCache(BatchJobStatusCache):

    def __init__(self):
        super(HTCondorJobStatusCache, self).__init__()

    def _ask_for_job_status(self, job_id: int = None):
        q_cmd = ["condor_q", "-json", "-attributes", "ClusterId,ProcId,JobStatus"]
        history_cmd = ["condor_history", "-json", "-attributes", "ClusterId,ProcId,JobStatus"]

        if job_id:
            q_cmd.append(str(job_id))
            output = subprocess.check_output(q_cmd)
        else:
            output = subprocess.check_output(q_cmd)
        output = output.decode()

        dict_list = []
        for status_dict in output.lstrip("[\n").rstrip("\n]\n").split("\n,\n"):
            if status_dict:
                dict_list.append(json.loads(status_dict))
            else:
                continue

        if job_id:
            if job_id not in [status_dict["ClusterId"] for status_dict in dict_list]:
                history_cmd.append(str(job_id))
                output = subprocess.check_output(history_cmd)
                output = output.decode()
                output = output.lstrip("[\n").rstrip("\n]\n").split("\n,\n")
                if output:
                    dict_list.append(json.loads(output))

        for status_dict in dict_list:
            self[status_dict["ClusterId"]] = status_dict["JobStatus"]  # int -> int


class HTCondorJobStatus(enum.IntEnum):
    idle = 1
    running = 2
    removed = 3
    completed = 4
    held = 5
    submission_err = 6


_batch_job_status_cache = HTCondorJobStatusCache()
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
