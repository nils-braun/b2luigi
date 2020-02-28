import subprocess
import shlex
from b2luigi.batch.processes import BatchProcess, JobStatus


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

        self.project_name = None

    def get_job_status(self):
        n_jobs_by_status = self._get_n_jobs_by_status()
        n_jobs = sum(n_jobs_by_status.values())
        n_done = n_jobs_by_status["Done"]
        n_failed = sum([n_jobs_by_status["Failed"],
                        n_jobs_by_status["Killed"],
                        n_jobs_by_status["Deleted"],
                        n_jobs_by_status["Stalled"]])
        n_being_processed = sum([n_jobs_by_status["Waiting"],
                                 n_jobs_by_status["Completed"],
                                 n_jobs_by_status["Running"]])
        assert n_failed + n_done + n_being_processed == n_jobs,\
            "Error in job categorization, numbers of jobs in cateries don't add up to total"

        if n_done > 0 and n_being_processed == 0:
            # TODO this also classsifies projects unsuccessful where some jobs failed
            return JobStatus.successful
        if n_failed == n_jobs:
            return JobStatus.aborted
        if n_jobs_by_status["Waiting"] == n_jobs:
            return JobStatus.idle
        if n_being_processed > 0:
            return JobStatus.running
        raise RuntimeError("Could not determine JobStatus")

    def start_job(self):
        command_str = (f"gbasf2 {self.wrapper_file_path} -f {self.pickle_file_path} -i {self.input_dataset} "
                       f" -p {self.project_name} -s {self.release}")
        command = shlex.split(command_str)
        print(f"\nSending jobs to grid via command:\n{command_str}\n")
        subprocess.run(command, check=True, env=self.gbasf2_env)
        self._send_to_grid()

    def kill_job(self):
        if not self._check_project_exists():
            return
        command = shlex.split(f"gb2_job_kill --force -p {self.project_name}")
        subprocess.run(command, check=True, env=self.gbasf2_env)

    def _check_project_exists(self) -> bool:
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

    def _download_dataset(self, output_directory) -> None:
        command = shlex.split(f"gb2_ds_get --force {self.project_name}")
        print("Downloading dataset with command ", " ".join(command))
        output = subprocess.check_output(command, env=self.gbasf2_env, encoding="utf-8", cwd=output_directory)
        print(output)
        if output.strip() == "No file found":
            raise RuntimeError(f"No output data for gbasf2 project {self.project_name} found.")
