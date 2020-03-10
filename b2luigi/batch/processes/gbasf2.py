import shlex
import subprocess
import time
from warnings import warn

from b2luigi.basf2_helper.utils import get_basf2_git_hash
from b2luigi.batch.processes import BatchProcess, JobStatus
from b2luigi.core.settings import get_setting

import basf2
import basf2.pickle_path as b2pp


class Gbasf2Process(BatchProcess):
    """
    Batch process for working with gbasf2 projects on grid.

    A BatchProcess job corresponds to a whole project in gbasf2.  The task of
    creating and managing jobs in a project is left to gbasf2.
    """

    gbasf2_setup_script = "setup_gbasf2.sh"

    @property
    def gbasf2_env(self):
        """
        Return dictionary with gbasf2 environment from ``self.gbasf2_setup_script``.

        Runs the script only once and then caches the result in ``self._cached_gbasf2_env``
        to prevent unnecessary password prompts.
        """
        if self._cached_gbasf2_env is None:
            print(f"Setting up environment, sourcing {self.gbasf2_setup_script}")
            command = shlex.split(f"env -i bash -c 'source {self.gbasf2_setup_script} > /dev/null && env'")
            output = subprocess.check_output(command, encoding="utf-8")
            self._cached_gbasf2_env = dict(line.split("=", 1) for line in output.splitlines())
        return self._cached_gbasf2_env

    #: cached gbasf2 enviromnent, initiallized and accessed via ``self.gbasf2_env``
    _cached_gbasf2_env = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.project_name = get_setting("gbasf2_project_name")

    def get_job_status(self):
        n_jobs_by_status = self._get_n_jobs_by_status()
        n_jobs = sum(n_jobs_by_status.values())
        n_done = n_jobs_by_status["Done"]
        n_failed = (n_jobs_by_status["Failed"] +
                    n_jobs_by_status["Killed"] +
                    n_jobs_by_status["Deleted"] +
                    n_jobs_by_status["Stalled"])
        n_being_processed = n_jobs_by_status["Running"] + n_jobs_by_status["Completed"]
        n_waiting = n_jobs_by_status["Waiting"]
        assert n_failed + n_done + n_waiting + n_being_processed == n_jobs,\
            "Error in job categorization, numbers of jobs in cateries don't add up to total"

        # At the moment, gbasf2 project success requires all sub-jobs to be have the "Done" status
        # TODO at settin for different success requirements
        # TODO maybe resubmit of partially successful jobs
        if n_done == n_jobs:
            # download dataset on job success, not sure if this is the best place to do so
            self._download_dataset()
            return JobStatus.successful
        if n_failed > 0:
            return JobStatus.aborted
        if n_waiting == n_jobs:
            return JobStatus.idle
        if n_being_processed > 0:
            return JobStatus.running
        raise RuntimeError("Could not determine JobStatus")

    def start_job(self):
        gbasf2_input_dataset = get_setting("gbasf2_input_dataset")
        gbasf2_release = get_setting("gbasf2_release", default=get_basf2_git_hash())

        pickle_file_path = "serialized_basf2_path.pkl"
        self._write_path_to_file(pickle_file_path)
        wrapper_file_path = "steering_file_wrapper.py"
        self._create_wrapper_steering_file(pickle_file_path, wrapper_file_path)

        command_str = (f"gbasf2 {wrapper_file_path} -f {pickle_file_path} -i {gbasf2_input_dataset} "
                       f" -p {self.project_name} -s {gbasf2_release}")
        command = shlex.split(command_str)
        print(f"\nSending jobs to grid via command:\n{command_str}\n")
        subprocess.run(command, check=True, env=self.gbasf2_env)

    def kill_job(self):
        if not self._check_project_exists():
            return
        command = shlex.split(f"gb2_job_kill --force -p {self.project_name}")
        subprocess.run(command, check=True, env=self.gbasf2_env)

    def _write_path_to_file(self, pickle_file_path):
        try:
            path = self.task.create_path()
        except AttributeError as err:
            print("Gbasf2 batch process can only used with tasks that generate basf2 paths with "
                  "a ``create_path()`` method, e.g. are an instance of ``Basf2PathTask``.")
            raise err
        path.add_module("Progress")
        b2pp.write_path_to_file(path, pickle_file_path)
        print(f"\nSaved serialized path in {pickle_file_path}\nwith content:\n")
        basf2.print_path(path)

    def _create_wrapper_steering_file(self, pickle_file_path, wrapper_file_path, max_event=0):
        with open(wrapper_file_path, "w") as wrapper_file:
            wrapper_file.write(f"""
import basf2
from basf2 import pickle_path as b2pp
path = b2pp.get_path_from_file("{pickle_file_path}")
basf2.print_path(path)
basf2.process(path, max_event={max_event})
print(basf2.statistics)
            """
                               )

    def _check_project_exists(self):
        command = shlex.split(f"gb2_job_status -p {self.project_name}")
        output = subprocess.check_output(command, encoding="utf-8", env=self.gbasf2_env).strip()
        if output == "0 jobs are selected.":
            return False
        if "--- Summary of Selected Jobs ---" in output:
            return True
        raise RuntimeError("Output of gb2_job_status did not contain expected strings,"
                           " could not determine if project exists")

    def _get_n_jobs_by_status(self):
        assert self._check_project_exists(), f"Project {self.project_name} doest not exist yet"

        command = shlex.split(f"gb2_job_status -p {self.project_name}")
        output = subprocess.check_output(command, encoding="utf-8", env=self.gbasf2_env)
        # get job summary dict in the form of e.g.
        # {'Completed': 0, 'Deleted': 0, 'Done': 255, 'Failed': 0,
        # 'Killed': 0, 'Running': 0, 'Stalled': 0, 'Waiting': 0}
        job_summary_string = output.splitlines()[-1].strip()
        print("Job summary:\n" + job_summary_string)
        n_jobs_by_status = dict((summary_substring.split(":", 1)[0].strip(),
                                 int(summary_substring.split(":", 1)[1].strip()))
                                for summary_substring in job_summary_string.split())
        status_keys = list(n_jobs_by_status.keys())
        status_keys.sort()
        assert status_keys == ['Completed', 'Deleted', 'Done', 'Failed',
                               'Killed', 'Running', 'Stalled', 'Waiting'],\
            "Error when obtaining job summary, it does not contain the required status keys"
        return n_jobs_by_status

    def _download_dataset(self, output_directory="."):
        command = shlex.split(f"gb2_ds_get --force {self.project_name}")
        print("Downloading dataset with command ", " ".join(command))
        output = subprocess.check_output(command, env=self.gbasf2_env, encoding="utf-8", cwd=output_directory)
        print(output)
        if output.strip() == "No file found":
            raise RuntimeError(f"No output data for gbasf2 project {self.project_name} found.")
