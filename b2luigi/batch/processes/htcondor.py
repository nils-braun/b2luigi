import json
import os
import re
import subprocess
import enum
import time

from retry import retry

from b2luigi.core.settings import get_setting
from b2luigi.batch.processes import BatchProcess, JobStatus
from b2luigi.batch.cache import BatchJobStatusCache
from b2luigi.core.utils import get_log_file_dir, get_task_file_dir
from b2luigi.core.executable import create_executable_wrapper


class HTCondorJobStatusCache(BatchJobStatusCache):

    @retry(subprocess.CalledProcessError, tries=3, delay=2, backoff=3)  # retry after 2,6,18 seconds
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
            try:
                output = subprocess.check_output(q_cmd)
            except:
                # somteimes it seems on naf the htcondor shedd is for a short time not available
                # first try to overcome this is by just wait a bit an try once again
                t_retry = 60
                print(f"check job status failed... try in {t_retry}s once more")
                time.sleep(t_retry)
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
    See https://htcondor.readthedocs.io/en/latest/classad-attributes/job-classad-attributes.html
    """
    idle = 1
    running = 2
    removed = 3
    completed = 4
    held = 5
    transferring_output = 6
    suspended = 7
    failed = 999


_batch_job_status_cache = HTCondorJobStatusCache()


class HTCondorProcess(BatchProcess):
    """
    Reference implementation of the batch process for a HTCondor batch system.

    Additional to the basic batch setup (see :ref:`batch-label`), additional
    HTCondor-specific things are:

    * Please note that most of the HTCondor batch farms do not have the same
      environment setup on submission and worker machines, so you probably want to give an
      ``env_script``, an ``env`` :meth:`setting <b2luigi.set_setting>` and/or a different ``executable``.

    * HTCondor supports copying files from submission to workers. This means if the
      folder of your script(s)/python project/etc. is not accessible on the worker, you can
      copy it from the submission machine by adding it to the setting ``transfer_files``.
      This list can host both folders and files.
      Please note that due to HTCondors file transfer mechanism, all specified folders
      and files will be copied into the worker node flattened, so if you specify
      `a/b/c.txt` you will end up with a file `c.txt`.
      If you use the ``transfer_files`` mechanism, you need to set the ``working_dir`` setting to "."
      as the files will end up in the current worker scratch folder.
      All specified files/folders should be absolute paths.

      .. hint::
        Please do not specify any parts or the full results folder. This will lead to unexpected
        behavior. We are working on a solution to also copy results, but until this the
        results folder is still expected to be shared.

      If you copy your python project using this setting to the worker machine, do not
      forget to actually set it up in your setup script.
      Additionally, you might want to copy your ``settings.json`` as well.

    * Via the ``htcondor_settings`` setting you can provide a dict as
      a for additional options, such as requested memory etc. Its value has to be a dictionary
      containing HTCondor settings as key/value pairs. These options will be written into the job
      submission file. For an overview of possible settings refer to the `HTCondor documentation
      <https://htcondor.readthedocs.io/en/latest/users-manual/submitting-a-job.html#>`_.

    * Same as for the :ref:`LSF`, the ``job_name`` setting allows giving a meaningful name to a
      group of jobs. If you want to be htcondor-specific, you can provide the ``JobBatchName`` as an
      entry in the ``htcondor_settings`` dict, which will override the global ``job_name`` setting.
      This is useful for manually checking the status of specific jobs with

      .. code-block:: bash

        condor_q -batch <job name>

    Example:

        .. literalinclude:: ../../examples/htcondor/htcondor_example.py
           :linenos:
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
        if job_status in [HTCondorJobStatus.idle, HTCondorJobStatus.running, HTCondorJobStatus.transferring_output,
                          HTCondorJobStatus.suspended]:
            return JobStatus.running
        if job_status in [HTCondorJobStatus.removed, HTCondorJobStatus.held, HTCondorJobStatus.failed]:
            return JobStatus.aborted
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
        os.makedirs(log_file_dir, exist_ok=True)

        stdout_log_file = os.path.abspath(os.path.join(log_file_dir, "stdout"))
        submit_file_content.append(f"output = {stdout_log_file}")

        stderr_log_file = os.path.abspath(os.path.join(log_file_dir, "stderr"))
        submit_file_content.append(f"error = {stderr_log_file}")

        job_log_file = os.path.abspath(os.path.join(log_file_dir, "job.log"))
        submit_file_content.append(f"log = {job_log_file}")

        # Specify the executable
        executable_file = create_executable_wrapper(self.task)
        submit_file_content.append(f"executable = {os.path.basename(executable_file)}")

        # Specify additional settings
        general_settings = get_setting("htcondor_settings", dict())
        try:
            general_settings.update(self.task.htcondor_settings)
        except AttributeError:
            pass

        transfer_files = get_setting("transfer_files", task=self.task, default=[])
        if transfer_files:
            working_dir = get_setting("working_dir", task=self.task, default="")
            if not working_dir or working_dir != ".":
                raise ValueError("If using transfer_files, the working_dir must be explicitely set to '.'")

            general_settings.setdefault("should_transfer_files", "YES")
            general_settings.setdefault("when_to_transfer_output", "ON_EXIT")

            transfer_files = set(transfer_files)

            for transfer_file in transfer_files:
                if os.path.abspath(transfer_file) != transfer_file:
                    raise ValueError(
                        "You should only give absolute file names in transfer_files!" +
                        f"{os.path.abspath(transfer_file)} != {transfer_file}"
                    )

            env_setup_script = get_setting("env_script", task=self.task, default="")
            if env_setup_script:
                # TODO: make sure to call it relatively
                transfer_files.add(os.path.abspath(env_setup_script))

            general_settings.setdefault("transfer_input_files", ",".join(transfer_files))

        job_name = get_setting("job_name", task=self.task, default=False)
        if job_name is not False:
            general_settings.setdefault("JobBatchName", job_name)

        for key, item in general_settings.items():
            submit_file_content.append(f"{key} = {item}")

        # Finally also start the process
        submit_file_content.append("queue 1")

        # Now we can write the submit file
        output_path = get_task_file_dir(self.task)
        submit_file_path = os.path.join(output_path, "job.submit")

        os.makedirs(output_path, exist_ok=True)

        with open(submit_file_path, "w") as submit_file:
            submit_file.write("\n".join(submit_file_content))

        return submit_file_path
