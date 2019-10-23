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
                {...}
                ,
                {...}
                ,
                {...}
            ]
        The {...} are the different dictionaries including the specified attributes.
        Sometimes it might happen that a job is completed in between the status checks. Then its final status
        can be found in the `condor_history` file (works mostly in the same way as `condor_q`).
        Both commands are used in order to find out the `JobStatus`.
        """
        # https://htcondor.readthedocs.io/en/latest/man-pages/condor_q.html
        q_cmd = ["condor_q", "-json", "-attributes", "ClusterId,JobStatus,ExitStatus"]

        if job_id:
            output = subprocess.check_output(q_cmd + [str(job_id)])
        else:
            output = subprocess.check_output(q_cmd)

        seen_ids = self._fill_from_output(output)

        # If the specified job can not be found in the condor_q output, we need to request its history
        if job_id and job_id not in seen_ids:
            # https://htcondor.readthedocs.io/en/latest/man-pages/condor_history.html
            history_cmd = ["condor_history", "-json", "-attributes", "ClusterId,JobStatus,ExitCode", "-match", "1", str(job_id)]
            output = subprocess.check_output(history_cmd)

            self._fill_from_output(output)

    def _fill_from_output(self, output):
        output = output.decode()

        seen_ids = set()

        if not output:
            return seen_ids

        for status_dict in json.loads(output):
            if status_dict["JobStatus"] == HTCondorJobStatus.completed and status_dict["ExitCode"]:
                self[status_dict["ClusterId"]] = HTCondorJobStatus.failed
            else:
                self[status_dict["ClusterId"]] = status_dict["JobStatus"]

            seen_ids.add(status_dict["ClusterId"])

        return seen_ids


class HTCondorJobStatus(enum.IntEnum):
    """
    See https://htcondor.readthedocs.io/en/latest/classad-attributes/job-classad-attributes.htmls
    """
    idle = 1
    running = 2
    removed = 3
    completed = 4
    held = 5
    failed = 999


_batch_job_status_cache = HTCondorJobStatusCache()


class HTCondorProcess(BatchProcess):
    """



HTCondor
........
To use HTCondor as batch system, you have to add the ``batch_system`` setting in the settings file like

.. code-block:: json

    {
        "batch_system": "htcondor",
        "result_path": "/path/to/results",
        "log_folder":  "/path/to/logs",
        "env_setup": "/path/to/env_setup.sh",
        "htcondor_settings": {
            "+ContainerOS": "Ubuntu1804"
        }

    }

Additionaly, you have to provide a ``env_setup`` script that sets up the working environment on the execution node
(in contrast to LSF, which uses the same environment as the submission node). It will be automatically sourced before
each ``luigi`` task is executed.
Another new setting is ``htcondor_settings``. This is a dictionary where you can specify settings that will be put in the
job submission file used by HTCondor. These same settings are used for all jobs (tasks) that are submitted to the
batch system. In the case above, ``"+ContainerOS": "Ubuntu1804"`` specifies an Ubuntu container used to execute the job
(Note: This setting is only meant as an example. What you want to specify here depends on your the HTCondor setup
you're using).

.. hint::
    Currently, the ``HTCondorProcess`` implementation assumes that your setup uses a shared file system. This means, that
    the environment setup script, the result and the log directory have to be placed somewhere the execution node has access
    to. In addition the executed python file has also to be accessible from the execution node.

If you have job (task) specific settings, you can specify them by adding a ``htcondor_settings`` attribute
to your task like this

.. literalinclude:: ../../tests/htcondor/htcondor_example_b2luigi_2.py
       :linenos:

By assigning a dictionary to the ``htcondor_settings`` attribute (line 8-11 and 25-28), you can specify e.g the number of cpus or the
required memory for the task. For an overview of possible settings refer to the
`HTCondor documentation <https://htcondor.readthedocs.io/en/latest/users-manual/submitting-a-job.html#>`_.
These settings have to valid HTCondor settings you would normally write into a job
submission file.

Currently, besides the actual task output, the ``results`` directory will also contain the HTCondor job submission file, the
executable wrapper and a copy of the settings file.

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
        if not self._batch_job_id:
            return JobStatus.aborted

        try:
            job_status = _batch_job_status_cache[self._batch_job_id]
        except KeyError:
            return JobStatus.aborted

        if job_status in [HTCondorJobStatus.completed]:
            return JobStatus.successful
        elif job_status in [HTCondorJobStatus.idle, HTCondorJobStatus.running]:
            return JobStatus.running
        elif job_status in [HTCondorJobStatus.removed, HTCondorJobStatus.held, HTCondorJobStatus.failed]:
            return JobStatus.aborted
        else:
            raise ValueError(f"Unknown HTCondor Job status: {job_status}")

    def start_job(self):
        submit_file = self._create_htcondor_submit_file()

        # HTCondor submit needs to be called in the folder of the submit file
        submit_file_dir, submit_file = os.path.split(submit_file)
        output = subprocess.check_output(["condor_submit", submit_file], cwd=submit_file_dir)

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
        submit_file_content.append(f"error = {stderr_log_file}")

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
