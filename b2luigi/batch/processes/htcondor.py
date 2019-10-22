import json
import logging
import os
import re
import shutil
import stat
import subprocess
import enum
import sys

from b2luigi.core.utils import get_setting, get_output_dirs, create_cmd_from_task
from b2luigi.batch.processes import BatchProcess, JobStatus, BatchJobStatusCache
from b2luigi.core.utils import get_log_file_dir


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

        # reassign task_cmd since the current python3 command provided by
        # the new environment should be used by default
        self.task_cmd, self.task_env = create_cmd_from_task(self.task, "python3")
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
        submit_file_path = self._create_htcondor_submit_file()

        submit_file_dir, submit_file = os.path.split(submit_file_path)
        cmd = ["condor_submit", submit_file]

        # apparently you have to be in the same directory as the submit file
        # to be able to submit jobs with htcondor
        cur_dir = os.getcwd()

        # have to copy settings file to job working directory since b2luigi has to
        # take the result path from it
        shutil.copyfile("settings.json", os.path.join(submit_file_dir, "settings.json"))
        output = subprocess.check_output(cmd, env=self.task_env, cwd=submit_file_dir)
        output = output.decode()
        match = re.search(r"[0-9]+\.", output)
        if not match:
            raise RuntimeError("Batch submission failed with output " + output)

        self._batch_job_id = int(match.group(0)[:-1])

    def kill_job(self):
        if not self._batch_job_id:
            return
        subprocess.check_call(["condor_rm", str(self._batch_job_id)], stdout=subprocess.DEVNULL)

    def _create_htcondor_submit_file(self):
        output_path = get_output_dirs(self.task, create_folder=True)
        submit_file_path = os.path.join(output_path, "job.submit")
        executable_wrapper_path = self._create_executable_wrapper()

        log_file_dir = get_log_file_dir(self.task)
        stderr_log_file = log_file_dir + "job.err"
        stdout_log_file = log_file_dir + "job.out"
        job_log_file = log_file_dir + "job.log"

        general_settings = get_setting("htcondor_settings", dict())

        try:
            job_settings = self.task.htcondor_settings
        except AttributeError:
            job_settings = {}

        with open(submit_file_path, "w") as submit_file:
            submit_file.write(f"""
            executable = {executable_wrapper_path}
            should_transfer_files = YES
            transfer_input_files = settings.json
            log = {job_log_file}
            output = {stdout_log_file}
            error = {stderr_log_file}
            """)

            for key, item in general_settings.items():
                submit_file.write(f"{key} = {item}\n")

            for key, item in job_settings.items():
                submit_file.write(f"{key} = {item}\n")

            submit_file.write(f"queue 1\n")

        return submit_file_path

    def _create_executable_wrapper(self):

        env_setup_path = get_setting("env_setup")

        if not os.path.isfile(env_setup_path):
            raise FileNotFoundError(f"Environment setup script {env_setup_path} does not exist.")

        output_path = get_output_dirs(self.task, create_folder=True)
        shell = get_setting("shell", "bash")

        executable_wrapper_path = os.path.join(output_path, "executable_wrapper.sh")

        condor_executable_cmd = " ".join(self.task_cmd)
        with open(executable_wrapper_path, "w") as exec_wrapper:
            exec_wrapper.write(f"#!/bin/{shell}\n")
            exec_wrapper.write(f"source {env_setup_path}\n")
            exec_wrapper.write("echo 'Starting executable'\n")
            exec_wrapper.write("cd /jwd\n")
            exec_wrapper.write(f"{condor_executable_cmd}\n")

        # make wrapper executable
        st = os.stat(executable_wrapper_path)
        os.chmod(executable_wrapper_path, st.st_mode | stat.S_IEXEC)

        return executable_wrapper_path


