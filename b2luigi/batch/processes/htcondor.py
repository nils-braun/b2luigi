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
                    for status_dict in output:
                        dict_list.append(json.loads(status_dict))

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

        self._batch_job_id = None

    def get_job_status(self):

        assert self._batch_job_id

        try:
            job_status = _batch_job_status_cache[self._batch_job_id]
        except KeyError:
            return JobStatus.aborted
        if job_status == HTCondorJobStatus.completed:
            return JobStatus.successful
        elif job_status == HTCondorJobStatus.idle:
            return JobStatus.running
        elif job_status == HTCondorJobStatus.running:
            return JobStatus.running
        elif job_status == HTCondorJobStatus.removed:
            return JobStatus.aborted
        elif job_status == HTCondorJobStatus.held:
            return JobStatus.aborted
        elif job_status == HTCondorJobStatus.submission_err:
            return JobStatus.aborted
        else:
            raise ValueError(f"Unknown HTCondor Job status: {job_status}")

    def start_job(self):
        submit_file_path = self.create_htcondor_submit_file()
        exec_cmd, curr_env = self.create_condor_executable_cmd()

        submit_file_dir, submit_file = os.path.split(submit_file_path)
        cmd = ["condor_submit", submit_file]

        # apparently you have to in the same directory as the submit file
        # to be able to submit jobs with htcondor
        cur_dir = os.getcwd()
        os.chdir(submit_file_dir)
        output = subprocess.check_output(cmd, env=curr_env)
        os.chdir(cur_dir)
        output = output.decode()
        match = re.search(r"[0-9]+\.", output)
        if not match:
            raise RuntimeError("Batch submission failed with output " + output)

        self._batch_job_id = int(match.group(0)[:-1])

    def kill_job(self):
        if not self._batch_job_id:
            return
        subprocess.check_call(["condor_rm", str(self._batch_job_id)], stdout=subprocess.DEVNULL)

    def create_htcondor_submit_file(self):
        output_path = get_output_dirs(self.task, create_folder=True)
        submit_file_path = os.path.join(output_path, "job.submit")
        executable_wrapper_path = self.create_executable_wrapper()

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
            submit_file.write(f"executable = {executable_wrapper_path}\n")

            submit_file.write(f"log = {job_log_file}\n")
            submit_file.write(f"output = {stdout_log_file}\n")
            submit_file.write(f"error = {stderr_log_file}\n")

            for key, item in general_settings.items():
                submit_file.write(f"{key} = {item}\n")

            for key, item in job_settings.items():
                submit_file.write(f"{key} = {item}\n")

            submit_file.write(f"queue 1\n")

        return submit_file_path

    def create_executable_wrapper(self):

        env_setup_path = get_setting("env_setup")

        if not os.path.isfile(env_setup_path):
            raise FileNotFoundError(f"Environment setup script {env_setup_path} does not exist.")

        output_path = get_output_dirs(self.task, create_folder=True)
        shell = get_setting("shell", "bash")

        executable_wrapper_path = os.path.join(output_path, "executable_wrapper.sh")

        condor_executable_cmd, _ = self.create_condor_executable_cmd()
        condor_executable_cmd = " ".join(condor_executable_cmd)
        with open(executable_wrapper_path, "w") as exec_wrapper:
            exec_wrapper.write(f"#!/bin/{shell}\n")
            exec_wrapper.write(f"source {env_setup_path}\n")
            exec_wrapper.write(f"{condor_executable_cmd}\n")

        st = os.stat(executable_wrapper_path)
        os.chmod(executable_wrapper_path, st.st_mode | stat.S_IEXEC)

        return executable_wrapper_path

    def create_condor_executable_cmd(self):

        filename = os.path.realpath(sys.argv[0])

        cmd = []
        if hasattr(self.task, "cmd_prefix"):
            cmd = self.task.cmd_prefix

        if hasattr(self.task, "executable"):
            executable = self.task.executable
        else:
            executable = get_setting("executable", ["python3"])
        cmd += executable

        cmd += [os.path.abspath(filename), "--batch-runner", "--task-id", self.task.task_id]

        if hasattr(self.task, "env"):
            env = self.task.env
        else:
            env = os.environ.copy()

        return cmd, env


