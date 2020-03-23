import os
import shlex
import subprocess

from b2luigi.basf2_helper.utils import get_basf2_git_hash
from b2luigi.batch.processes import BatchProcess, JobStatus
from b2luigi.core.settings import get_setting
from b2luigi.core.utils import get_log_file_dir
from jinja2 import Template

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
        # TODO maybe make sure that project name is unique in some way for the chosen set of luigi parameters
        self.project_name = get_setting("gbasf2_project_name", task=self.task)
        self.pickle_file_name = "serialized_basf2_path.pkl"
        self.wrapper_file_name = "steering_file_wrapper.py"

    def get_job_status(self):
        """
        Exract the status of all sub-jobs in a gbasf2 project from
        ``gb2_job_status`` and return an overall project status, e.g. if all
        jobs are done return successful
        """
        # If project is does not exist on grid yet, so can't query for gbasf2 project status
        if not self._check_project_exists():
            return JobStatus.idle

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

        if n_waiting == n_jobs:
            return JobStatus.running
        if n_being_processed > 0:
            return JobStatus.running

        # Setting for the success requirement of a gbasf2 project, e.g. if all
        # sub-jobs need to be done or to allow for some failed jobs
        gbasf2_require_all_jobs_done = get_setting("gbasf2_require_all_jobs_done", default=True, task=self.task)

        if gbasf2_require_all_jobs_done:
            # Require all jobs to be done for project success, any job failure results in a failed project
            if n_done == n_jobs:
                # download dataset on job success
                self._download_dataset()
                return JobStatus.successful
            if n_failed > 0:
                return JobStatus.aborted
        else:
            # in this case require allow for some failed jobs, return project
            # success if some jobs were successful and no jobs are running
            # anymore, even if some jobs in project failed, as long as not all failed.
            if n_done > 0 and n_being_processed + n_waiting == 0:
                self._download_dataset()
                return JobStatus.successful
            if n_failed == n_jobs:
                return JobStatus.aborted
        # TODO reschedule failed jobs via ``gb2_job_reschedule``
        # TODO think of better way to download dataset on project succcess than as sideffect in this function
        raise RuntimeError("Could not determine JobStatus")

    def start_job(self):
        log_file_dir = get_log_file_dir(self.task)
        os.makedirs(log_file_dir, exist_ok=True)
        gbasf2_input_dataset = get_setting("gbasf2_input_dataset", task=self.task)
        gbasf2_release = get_setting("gbasf2_release", default=get_basf2_git_hash(), task=self.task)

        self._write_path_to_file()
        self._create_wrapper_steering_file()

        command_str = (f"gbasf2 {self.wrapper_file_name} -f {self.pickle_file_name} -i {gbasf2_input_dataset} "
                       f" -p {self.project_name} -s {gbasf2_release}")
        command = shlex.split(command_str)
        print(f"\nSending jobs to grid via command:\n{command_str}\n")
        subprocess.run(command, check=True, env=self.gbasf2_env)

    def kill_job(self):
        """Kill gbasf2 project"""
        if not self._check_project_exists():
            return
        # Note: The two commands ``gb2_job_delete`` and ``gb2_job_kill`` differ
        # in that deleted jobs are killed and removed from the job database,
        # while only killed jobs can be restarted.
        command = shlex.split(f"gb2_job_delete --force -p {self.project_name}")
        subprocess.run(command, check=True, env=self.gbasf2_env)

    def _write_path_to_file(self):
        """
        Serialize and save the ``basf2.Path`` returned by ``self.task.create_path()`` to a python pickle file.
        """
        try:
            path = self.task.create_path()
        except AttributeError as err:
            print("Gbasf2 batch process can only used with tasks that generate basf2 paths with "
                  "a ``create_path()`` method, e.g. are an instance of ``Basf2PathTask``.")
            raise err
        path.add_module("Progress")
        b2pp.write_path_to_file(path, self.pickle_file_name)
        print(f"\nSaved serialized path in {self.pickle_file_name}\nwith content:\n")
        basf2.print_path(path)

    def _create_wrapper_steering_file(self):
        """
        Create a steering file to send to the grid that executes the pickled
        basf2 path from ``self.task.create_path()``.
        """
        # read a jinja2 template for the steerinfile that should execute the pickled path
        file_dir = os.path.dirname(os.path.realpath(__file__))
        template_file_path = os.path.join(file_dir, "templates/gbasf2_steering_file_wrapper.jinja2")
        with open(template_file_path, "r") as template_file:
            template = Template(template_file.read())
            # replace some variable values in the template
            steering_file_content = template.render(
                pickle_file_path=self.pickle_file_name,
                max_event=get_setting("max_event", default=0, task=self.task),
            )
        # write the template with the replacements to a new file which should be sent to the grid
        with open(self.wrapper_file_name, "w") as wrapper_file:
            wrapper_file.write(steering_file_content)

    def _check_project_exists(self):
        """
        Check if we can find the project on the grid with gb2_job_status.
        """
        command = shlex.split(f"gb2_job_status -p {self.project_name}")
        output = subprocess.check_output(command, encoding="utf-8", env=self.gbasf2_env).strip()
        if output == "0 jobs are selected.":
            return False
        if "--- Summary of Selected Jobs ---" in output:
            return True
        raise RuntimeError("Output of gb2_job_status did not contain expected strings,"
                           " could not determine if project exists")

    def _get_n_jobs_by_status(self):
        """
        Returns a dictionary with different gbasf2 job status as keys and the number of jobs in each category as values.
        """
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
        """Download the results from a gbasf2 project, stored as a dataset on the grid."""
        command = shlex.split(f"gb2_ds_get --force {self.project_name}")
        print("Downloading dataset with command ", " ".join(command))
        output = subprocess.check_output(command, env=self.gbasf2_env, encoding="utf-8", cwd=output_directory)
        print(output)
        if output.strip() == "No file found":
            raise RuntimeError(f"No output data for gbasf2 project {self.project_name} found.")
        # TODO: in the output dataset there is a root file created for each
        # file in the input dataset.The output files are have numbers added to
        # the filenames specified by the file names e.g. in the ``RootOutput``
        # and ``VariablesToNtuple`` modules. That makes it hard for the user to
        # define the output requirements in the ``output`` method of his task.
        # So maybe merge the output files or do something else to facilitate
        # defining outputs and checking that job is complete.
