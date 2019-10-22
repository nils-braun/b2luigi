import json
import logging
import os
import re
import shutil
import stat
import subprocess
import enum
import sys

from b2luigi.core.settings import get_setting
from b2luigi.batch.processes import BatchProcess, JobStatus
from b2luigi.batch.cache import BatchJobStatusCache
from b2luigi.core.utils import get_log_file_dir, get_task_file_dir
from b2luigi.core.executable import create_executable_wrapper


class HTCondorJobStatusCache(BatchJobStatusCache):

    def __init__(self):
        super(HTCondorJobStatusCache, self).__init__()

    def _ask_for_job_status(self, job_id: int = None):
        """
        With HTCondor, you can check the progress of your jobs using the `condor_q` command.
        If no `JobId` is given as argument, this command shows you the status of all queued jobs
        (usually only your own by default).

        Normally the HTCondor `JobID` is stated as `ClusterId.ProcId`. Since only on job is queued per
        cluster, we can identify jobs by their `ClusterId` (The `ProcId` will be 0 for all submitted jobs).
        With the `-json` option, the `condor_q` output is returned in the JSON format. By specifying some
        attributes, not the entire job ClassAd is returned, but only the necessary information to match a
        job to its `JobStatus`. The output is given as `string` and cannot be directly parsed into a json
        dictionary. It has the following form:
            [
                {....}
                ,
                {...}
                ,

                {...}
            ]
        The {....} are the different dictionaries including the specified attributes.
        Sometimes it might happen that a job is completed in between the status checks. Then its final status
        can be found in the `condor_history` file (works mostly in the same way as `condor_q`.
        Both commands are used in order to find the `JobStatus`.
        """
        # https://htcondor.readthedocs.io/en/latest/man-pages/condor_q.html
        q_cmd = ["condor_q", "-json", "-attributes", "ClusterId,ProcId,JobStatus"]
        # https://htcondor.readthedocs.io/en/latest/man-pages/condor_history.html
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
                    for status_dict in output:
                        dict_list.append(json.loads(status_dict))

        for status_dict in dict_list:
            self[status_dict["ClusterId"]] = status_dict["JobStatus"]  # int -> int


class HTCondorJobStatus(enum.IntEnum):
    """
    See https://htcondor.readthedocs.io/en/latest/classad-attributes/job-classad-attributes.htmls
    """
    idle = 1
    running = 2
    removed = 3
    completed = 4
    held = 5


_batch_job_status_cache = HTCondorJobStatusCache()


class HTCondorProcess(BatchProcess):
    """
    Reference implementation of the batch process for a HTCondor batch system.

    We assume that the batch system shares a file system with the submission node you
    are currently working on (or at least the current folder and the result/log directory
    are also available there with the same path).
    An environment has to be set up on its own for each job. For that, a environment
    setup script has to be provided in the ``settings.json`` file.
    If no own python cmd is specified, the task is executed with the current ``python3``
    available after the environment is setup.
    General settings (may be depended on your HTCondor setup) that affect all jobs (tasks)
    can be specified in the ``settings.json`` by adding a ``htcondor_settings`` entry.
    Job specific settings, e.g. number of cpus or required memory can be specified by adding
    a ``htcondor_settings`` attribute to the task. It's value has to be a dictionary containing
    also HTCondor settings as key/value pairs.
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

        if job_status in [HTCondorJobStatus.completed]:
            return JobStatus.successful
        elif job_status in [HTCondorJobStatus.idle, HTCondorJobStatus.running]:
            return JobStatus.running
        elif job_status in [HTCondorJobStatus.removed, HTCondorJobStatus.held]:
            return JobStatus.aborted
        else:
            raise ValueError(f"Unknown HTCondor Job status: {job_status}")

    def start_job(self):
        submit_file = self._create_htcondor_submit_file()
        command = ["condor_submit", submit_file]

        # TODO maybe submit_file_dir, submit_file = os.path.split(submit_file_path)
        output = subprocess.check_output(command)

        output = output.decode()
        match = re.search(r"[0-9]+\.", output)
        if not match:
            raise RuntimeError("Batch submission failed with output " + output)

        self._batch_job_id = int(match.group(0)[:-1])
        

    def kill_job(self):
        if not self._batch_job_id:
            return

        subprocess.run(["condor_rm", str(self._batch_job_id)], stdout=subprocess.DEVNULL)

    def _create_htcondor_submit_file(self):
        submit_file_content = []

        # Specify where to write the log to
        log_file_dir = get_log_file_dir(self.task)

        stdout_log_file = log_file_dir + "stdout"
        submit_file_content.append(f"output = {stdout_log_file}")

        stderr_log_file = log_file_dir + "stderr"
        submit_file_content.append(f"error = {stderr_log_file})")

        job_log_file = log_file_dir + "job.log"
        submit_file_content.append(f"log = {job_log_file}")

        # Specify the executable
        executable_file = create_executable_wrapper(self.task)
        submit_file_content.append(f"executable = {executable_file}")

        # Specify additional settings
        general_settings = get_setting("htcondor_settings", dict())
        try:
            general_settings.update(self.task.htcondor_settings)
        except AttributeError:
            pass
        
        for key, item in general_settings.items():
            submit_file_content.append(f"{key} = {item}")

        # Finally also start the process
        submit_file_content.append("queue 1")

        # Now we can write the submit file
        output_path = get_task_file_dir(self.task)
        submit_file_path = os.path.join(output_path, "job.submit")

        with open(submit_file_path, "w") as submit_file:
            submit_file.write("\n".join(submit_file_content))

        return submit_file_path
