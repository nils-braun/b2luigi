import json
import re
import subprocess

from b2luigi.batch.processes import BatchProcess, JobStatus
from b2luigi.core.utils import get_log_file_dir

from cachetools import TTLCache


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
