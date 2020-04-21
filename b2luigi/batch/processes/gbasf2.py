from datetime import datetime, timedelta
from functools import lru_cache
import os
from collections import Counter
import json
import shlex
import shutil
import subprocess
from subprocess import PIPE
import tempfile
import warnings

from b2luigi.basf2_helper.utils import get_basf2_git_hash
from b2luigi.batch.processes import BatchProcess, JobStatus
from b2luigi.core.settings import get_setting
from b2luigi.core.utils import get_log_file_dir, get_task_file_dir
from jinja2 import Template

import basf2
import basf2.pickle_path as b2pp


class Gbasf2Process(BatchProcess):
    """
    Batch process for working with gbasf2 projects on the *LHC Computing Grid* (LCG).

    **What it does**

    gbasf2 project submission
        The gbasf2 batch process takes the basf2 path returned by the ``create_path()``
        method of the task, saves it to the disk and creates a wrapper steering file that
        executes the saved path. It then sends both to the LCG via
        the BelleII-specific Dirac-wrapper gbasf2.

    Project status monitoring
        After the project submission, the gbasf batch process regularly checks the status
        of all the jobs belonging to a gbasf2 project returns a success if
        all jobs had been successful, while a single failed job results in a failed project.

    Download of datasets and logs
        If all jobs had been successful, it automatically downloads the output dataset and
        the log files from the job sandboxes.


    .. note::
        **Limitations**

        - The gbasf2 batch process for luigi can only be used for tasks
          inhereting from ``Basf2PathTask`` or other tasks with a
          ``create_path()`` method that returns a basf2 path.
        - It can only be used for pickable/serializable basf2 paths, as it stores
          the path created by ``create_path`` in a python pickle file and runs that on the grid.
        - basf2 variable aliases are at the moment not supported, as they are not included in the pickled path

    **Usage**

    Settings for gbasf2 tasks
        To submit a task with the gbasf2 wrapper, you first you have to add the property
        ``batch_system = "gbasf2"``, which sets the ``batch_system`` setting.
        It is not recommended to set that setting globally, as not all tasks can be submitted to the grid,
        but only tasks with a ``create_path`` method.

        For gbasf2 tasks it is further required to set the settings

        - ``gbasf2_input_dataset``: The input dataset on the grid to use.
        - ``gbasf2_project_name_prefix``: A string with which your gbasf2 project names will start.
          To ensure the project associate with each unique task (i.e. for each of luigi parameters)
          is unique, a hash generated from the luigi parameters of the task will be appended to the prefix
          to create the actual gbasf2 project name.

        For example:

        .. code-block:: python

            class MyTask(Basf2PathTask):
                batch_system = "gbasf2"
                gbasf2_project_name_prefix = b2luigi.Parameter(significant=False)
                gbasf2_input_dataset = b2luigi.Parameter(hashed=True)

        The following settings are not required as they have default values, but those might not work for you:

        - ``gbasf2_install_directory``: If your gbasf2 install location is not ``~/gbasf2KEK``, you have set this,
          so that the gbasf2 batch process can set up the gbasf2 environment for its gbasf2 commands.
        - ``gbasf2_download_directory``: Defines in which directory the ``gb2_ds_get`` download will called
          to download the result dataset.
          The output will then be in the in a subdirectory of that directory with the name of the project.
          It defaults to the current directory.
        - ``gbasf2_release``: Set this if you want the jobs to use another release on the grid than your
          currently set up release, which is the default.

        By setting ``gbasf2_print_status_updates`` to ``False`` you can turn off the printing of of the job summaries,
        that is the number of jobs in different states in a gbasf2 project.

        The following optional settings correspond to the equally named ``gbasf`` command line options
        (without the ``gbasf_`` prefix) that you can set to customize your gbasf2 project:

        ``gbasf2_additional_files``,
        ``gbasf2_n_repition_job``,
        ``gbasf2_force_submission``,
        ``gbasf2_cputime``,
        ``gbasf2_evtpersec``,
        ``gbasf2_priority``,
        ``gbasf2_jobtype``,
        ``gbasf2_basf2opt``

       It is further possible to append arbitrary command line arguments to the ``gbasf2`` submission command
       with the ``gbasf2_additional_params`` setting.
       If you want to blacklist a grid site, you can e.g. add

       .. code-block::
          b2luigi.set_setting("gbasf2_additional_params",  "--banned_site LCG.KEK.jp")


    Example
        Here is an example file to submit an analysis path created by the script in
        ``examples/gbasf2/example_mdst_analysis`` to grid via gbasf2:

        .. literalinclude:: ../../examples/gbasf2/gbasf2_example.py
           :caption: File: ``examples/gbasf2/gbasf2_example.py``
           :linenos:

    Handling failed jobs
        The gbasf2 input wrapper considers the gbasf2 project as failed if any of
        the jobs in the project failed.  It the automatically downloads the logs, so
        please look into them to see what the reason was. You then need to resubmit them
        manually with the ``gb2_job_reschedule`` or delete them with
        ``gb2_job_delete`` so that the gbasf2 batch process doesn't know they ever
        existed. Then run just run your luigi task/script again.

    .. note::
        **Note on nomenclature:**
        The ``BatchProcess`` is intended to work with jobs, thus is has method names such as
        ``get_job_status``. The gbasf2 implementation of that class is special in that it wraps
        gbasf2 projects, of which each contains multiple grid jobs, one for each file in the input
        dataset. The actual (sub-) job handling is left to gbasf2. So the job status of the batch process
        refers to the status of the whole project.


    **Planned features**

    - automatically reschedule failed jobs until a maximum number of retries is reached
    - add some helper functionality to deal with output, to avoid having to redefine
      task output when switching between local batch process and gbasf2 batch process.
    """

    # directory of the file in which this class is defined
    _file_dir = os.path.dirname(os.path.realpath(__file__))

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # gbasf2 project name, must be property/attribute, e.g. a luigi parameter
        # Setting it via a setting.json file is not supported to make sure users set unique project names
        self.gbasf2_project_name = get_unique_gbasf2_project_name(self.task)

        # Output file directory of the task to wrap with gbasf2, where we will
        # store the pickled basf2 path and the created steerinfile to execute
        # that path.
        task_file_dir = get_task_file_dir(self.task)
        os.makedirs(task_file_dir, exist_ok=True)
        #: file name in which the pickled basf2 path from ``self.task.create_path()`` will be stored
        self.pickle_file_path = os.path.join(task_file_dir, "serialized_basf2_path.pkl")
        #: file name for steering file that executes pickled path, which will be send to the grid
        self.wrapper_file_path = os.path.join(task_file_dir, "steering_file_wrapper.py")

        self.log_file_dir = get_log_file_dir(self.task)
        os.makedirs(self.log_file_dir, exist_ok=True)

        # Store dictionary with n_jobs_by_status in attribute to check if it changed,
        # useful for printing job status on change only
        self._n_jobs_by_status = ""

        # Store whether the job had already been successful in a variable b/c
        # there's actions we want to do only the first time that
        # ``get_job_status`` returns a success.
        self._project_had_been_successful = False

        # get dirac user name
        proxy_info_str = self.setup_dirac_proxy()
        self.dirac_user = None
        for line in proxy_info_str.splitlines():
            if line.startswith("username"):
                self.dirac_user = line.split(":", 1)[1].strip()

    @property
    @lru_cache(maxsize=None)
    def gbasf2_env(self):
        """
        Return the gbasf2 environment dict.

        When first called, it executes the setup script from the
        ``gbasf2_install_directory`` then caches and returns the resulting
        environment.  This property can be used as the ``env`` parameter in
        subprocess calls, to execute gbasf2 commands in this environment
        """

        gbasf2_install_dir = get_setting("gbasf2_install_directory", default="~/gbasf2KEK", task=self.task)
        gbasf2_setup_path = os.path.join(gbasf2_install_dir, "BelleDIRAC/gbasf2/tools/setup")
        if not os.path.isfile(os.path.expanduser(gbasf2_setup_path)):
            raise FileNotFoundError(
                f"Could not find gbasf2 setup files in ``{gbasf2_install_dir}``.\n" +
                "Make sure to that gbasf2 is installed at the location specified by the " +
                "``gbasf2_install_dir`` setting."
            )
        # complete bash command to set up the gbasf2 environment
        # piping output to /dev/null, because we want that our final script only prints the ``env`` output
        gbasf2_setup_command_str = f"source {gbasf2_setup_path} > /dev/null"
        # command to execute the gbasf2 setup command in a fresh shell and output the produced environment
        echo_gbasf2_env_command = shlex.split(f"env -i bash -c '{gbasf2_setup_command_str} > /dev/null && env'")
        gbasf2_env_string = subprocess.run(echo_gbasf2_env_command, check=True, stdout=PIPE, encoding="utf-8").stdout
        gbasf2_env = dict(line.split("=", 1) for line in gbasf2_env_string.splitlines())
        return gbasf2_env

    def setup_dirac_proxy(self):
        """
        Runs ``gb2_proxy_init -g belle`` if necessary and returns
        ``gb2_proxy_info`` output

        ``gb2_proxy_init`` has to be run only once every 24 hours for a
        terminal.  So we first check with ``gb2_proxy_info`` and if an old proxy
        is still valid, don't re-run the command to prevent unnecessary
        certificate password prompts
        """
        # Get time that the proxy is still valid from the gb2_proxy_info output line "timeleft".
        # If no proxy had been initialized, the output will not contain the "timeleft" string.
        # Alternatively, if the proxy time ran out, the timeleft value will be 00:00:00
        proxy_info_str = subprocess.run(["gb2_proxy_info"], check=True, env=self.gbasf2_env, stdout=PIPE, encoding="utf-8").stdout
        for line in proxy_info_str.splitlines():
            if line.startswith("timeleft"):
                timeleft_str = line.split(":", 1)[1].strip()
                timeleft = datetime.strptime(timeleft_str, '%H:%M:%S')
                timeleft_delta = timedelta(hours=timeleft.hour, minutes=timeleft.minute, seconds=timeleft.second)
                if timeleft_delta.total_seconds() > 0:
                    return proxy_info_str
        # initiallize proxy
        subprocess.run(shlex.split("gb2_proxy_init -g belle"), check=True, env=self.gbasf2_env)
        new_proxy_info_str = subprocess.run(
            ["gb2_proxy_info"], check=True, env=self.gbasf2_env, stdout=PIPE, encoding="utf-8").stdout
        return new_proxy_info_str

    def get_job_status(self):

        """
        Get overall status of the gbasf2 project.

        First obtain the status of all (sub-) jobs in a gbasf2 project, similar
        to ``gb2_job_status``, and return an overall project status, e.g. when
        all jobs are done, return ``JobStatus.successful`` to show that the
        gbasf2 project succeeded.

        The status of each individual job can be one of::

            [Submitting, Submitted, Received, Checking, Staging, Waiting, Matched, Rescheduled,
             Running, Stalled, Completing, Done, Completed, Failed, Deleted, Killed]

        (Taken from  https://github.com/DIRACGrid/DIRAC/blob/rel-v7r1/WorkloadManagementSystem/Client/JobStatus.py)

        """
        # If project is does not exist on grid yet, so can't query for gbasf2 project status
        if not self._check_project_exists():
            return JobStatus.idle

        job_status_dict = self._get_job_status_dict()
        n_jobs_by_status = Counter()
        for _, job_info in job_status_dict.items():
            n_jobs_by_status[job_info["Status"]] += 1

        # print summary of jobs in project if setting is set and job status changed
        if (get_setting("gbasf2_print_status_updates", default=True, task=self.task) and
                n_jobs_by_status != self._n_jobs_by_status):
            time_string = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            job_status_string = str(dict(sorted(n_jobs_by_status.items()))).strip("{}")
            print(f"Jobs in gbasf2 project \"{self.gbasf2_project_name}\" at {time_string}: {job_status_string}")
        self._n_jobs_by_status = n_jobs_by_status

        n_jobs = len(job_status_dict)
        n_done = n_jobs_by_status["Done"]
        # note: "Killed" jobs also get moved to "Failed" after a while
        n_failed = n_jobs_by_status["Failed"]
        n_in_final_state = n_done + n_failed

        # at the moment we have a hard criteria that if one job in project failed, the task is considered failed
        if n_failed > 0:
            self._on_failure_action()
            return JobStatus.aborted

        if n_in_final_state < n_jobs:
            return JobStatus.running

        # Require all jobs to be done for project success, any job failure results in a failed project
        if n_done == n_jobs:
            # download dataset only the first time that we return JobStatus.successful
            if not self._project_had_been_successful:
                self._on_first_success_action()
                self._project_had_been_successful = True
            return JobStatus.successful

        raise RuntimeError("Could not determine JobStatus")

    def _on_first_success_action(self):
        """
        Things to do after all jobs in the project had been successful, e.g. downloading the dataset and logs
        """
        self._download_dataset()
        self._download_logs()

    def _on_failure_action(self):
        """
        Things to do after the project failed
        """
        job_status_dict = self._get_job_status_dict()
        failed_job_dict = {job_id: job_info for job_id, job_info in job_status_dict.items()
                           if job_info["Status"] == "Failed"}
        n_failed = len(failed_job_dict)
        print(f"{n_failed} failed jobs:\n{failed_job_dict}")

        self._download_logs()

    def start_job(self):
        """Submit new gbasf2 project to grid"""
        if self._check_project_exists():
            warnings.warn(
                f"\nProject with name {self.gbasf2_project_name} already exists on grid, "
                "therefore not submitting new project. If you want to submit a new project, "
                "change the project name."
            )
            return

        self._write_path_to_file()
        self._create_wrapper_steering_file()

        # submit gbasf2 project
        gbasf2_command = self._build_gbasf2_submit_command()
        print("\nSending jobs to grid via command:\n", " ".join(gbasf2_command))
        subprocess.run(gbasf2_command, check=True, env=self.gbasf2_env)

    def kill_job(self):
        """Kill gbasf2 project"""
        if not self._check_project_exists():
            return
        # Note: The two commands ``gb2_job_delete`` and ``gb2_job_kill`` differ
        # in that deleted jobs are killed and removed from the job database,
        # while only killed jobs can be restarted.
        command = shlex.split(f"gb2_job_kill --force --user {self.dirac_user} -p {self.gbasf2_project_name}")
        subprocess.run(command, check=True, env=self.gbasf2_env)

    def _build_gbasf2_submit_command(self):
        """
        Function to create the gbasf2 submit command to pass to subprocess.run
        from the task options and attributes.
        """
        gbasf2_release = get_setting("gbasf2_release", default=get_basf2_git_hash(), task=self.task)

        gbasf2_additional_files = get_setting("gbasf2_additional_files", default=False, task=self.task)
        if gbasf2_additional_files is not False:
            # make sure that gbasf2_additional_files is not a string, for which " ".join will yield unexpected results
            assert not isinstance(gbasf2_additional_files, str), "gbasf2_additional_files should be a list or tuple, not a string."
            additional_files_str = " ".join(gbasf2_additional_files)
        else:
            additional_files_str = ""

        gbasf2_command_str = (f"gbasf2 {self.wrapper_file_path} -f {self.pickle_file_path} {additional_files_str} " +
                              f"-p {self.gbasf2_project_name} -s {gbasf2_release} ")

        gbasf2_input_dataset = get_setting("gbasf2_input_dataset", default=False, task=self.task)
        if gbasf2_input_dataset is not False:
            gbasf2_command_str += f" -i {gbasf2_input_dataset} "

        gbasf2_n_repition_jobs = get_setting("gbasf2_n_repition_job", default=False, task=self.task)
        if gbasf2_n_repition_jobs is not False:
            gbasf2_command_str += f" --repetition {gbasf2_n_repition_jobs} "

        # now add some additional optional options to the gbasf2 job submission string

        # whether to ask user for confirmation before submitting job
        force_submission = get_setting("gbasf2_force_submission", default=True, task=self.task)
        if force_submission:
            gbasf2_command_str += " --force "

        # estimated cpu time per sub-job in minutes
        cpu_minutes = get_setting("gbasf2_cputime", default=False, task=self.task)
        if cpu_minutes is not False:
            gbasf2_command_str += f" --cputime {cpu_minutes} "

        # estimated number or processed events per second
        evtpersec = get_setting("gbasf2_evtpersec", default=False, task=self.task)
        if evtpersec is not False:
            gbasf2_command_str += f" --evtpersec {evtpersec} "

        # gbasf2 job priority
        priority = get_setting("gbasf2_priority", default=False, task=self.task)
        if priority is not False:
            assert 0 <= priority <= 10, "Priority should be integer between 0 and 10."
            gbasf2_command_str += f" --priority {priority} "

        # gbasf2 job type (e.g. User, Production, ...)
        jobtype = get_setting("gbasf2_jobtype", default=False, task=self.task)
        if jobtype is not False:
            gbasf2_command_str += f" --jobtype {jobtype} "

        # additional basf2 options to use on grid
        basf2opt = get_setting("gbasf2_basf2opt", default=False, task=self.task)
        if basf2opt is not False:
            gbasf2_command_str += f" --basf2opt='{basf2opt}' "

        # optional string of additional parameters to append to gbasf2 command
        gbasf2_additional_params = get_setting("gbasf2_additional_params", default=False, task=self.task)
        if basf2opt is not False:
            gbasf2_command_str += f" {gbasf2_additional_params} "

        gbasf2_command = shlex.split(gbasf2_command_str)
        return gbasf2_command

    def _write_path_to_file(self):
        """
        Serialize and save the ``basf2.Path`` returned by ``self.task.create_path()`` to a python pickle file.
        """
        try:
            path = self.task.create_path()
        except AttributeError as err:
            raise Exception(
                "Gbasf2 batch process can only used with tasks that generate basf2 paths with "
                "a ``create_path()`` method, e.g. are an instance of ``Basf2PathTask``."
            ) from err

        path.add_module("Progress")
        b2pp.write_path_to_file(path, self.pickle_file_path)

    def _create_wrapper_steering_file(self):
        """
        Create a steering file to send to the grid that executes the pickled
        basf2 path from ``self.task.create_path()``.
        """
        # read a jinja2 template for the steerinfile that should execute the pickled path
        template_file_path = os.path.join(self._file_dir, "templates/gbasf2_steering_file_wrapper.jinja2")
        with open(template_file_path, "r") as template_file:
            template = Template(template_file.read())
            # replace some variable values in the template
            steering_file_stream = template.stream(
                pickle_file_path=self.pickle_file_path,
                max_event=get_setting("max_event", default=0, task=self.task),
            )
            # write the template with the replacements to a new file which should be sent to the grid
            steering_file_stream.dump(self.wrapper_file_path)

    def _check_project_exists(self):
        """
        Check if we can find the project on the grid with gb2_job_status.
        """
        command = shlex.split(f"gb2_job_status --user {self.dirac_user} -p {self.gbasf2_project_name}")
        output = subprocess.run(command, check=True, stdout=PIPE, encoding="utf-8", env=self.gbasf2_env).stdout
        if output.strip() == "0 jobs are selected.":
            return False
        if "--- Summary of Selected Jobs ---" in output:
            return True
        raise RuntimeError("Output of gb2_job_status did not contain expected strings,"
                           " could not determine if project exists")

    def _get_job_status_dict(self):
        """
        Returns a dictionary for all jobs in the project with a structure like the following,
        which I have taken and adapted from an example output::

            {
                "<JobID>": {
                    "SubmissionTime": "2020-03-27 13:08:49",
                    "Owner": "<dirac username>",
                    "JobGroup": "<ProjectName>",
                    "ApplicationStatus": "Done",
                    "HeartBeatTime": "2020-03-27 16:01:39",
                    "Site": "LCG.KEK.jp",
                    "MinorStatus": "Execution Complete",
                    "LastUpdateTime": "2020-03-27 16:01:40",
                    "Status": "<Job Status>"
                }
            ...
            }

        For that purpose, the script in ``gbasf2_job_status.py`` is called.
        That script directly interfaces with Dirac via its API, but it only works with the
        gbasf2 environment and python2, which is why it is called as a subprocess.
        The job status dictionary is passed to this function via json.
        """
        assert self._check_project_exists(), f"Project {self.gbasf2_project_name} doest not exist yet"

        job_status_script_path = os.path.join(self._file_dir, "gbasf2_utils/gbasf2_job_status.py")
        job_status_command = shlex.split(f"python2 {job_status_script_path} -p {self.gbasf2_project_name} -u {self.dirac_user}")
        job_status_json_string = subprocess.run(
            job_status_command, check=True, stdout=PIPE, encoding="utf-8", env=self.gbasf2_env
        ).stdout
        job_status_dict = json.loads(job_status_json_string)
        return job_status_dict

    def _download_dataset(self):
        """
        Download the results from a gbasf2 project, stored as a dataset on the grid.
        """
        # Get list of files that we want to download from the grid via ``gb2_ds_list`` so that we can
        # then compare this list with the results of the download to see if it was successful
        ds_list_command = shlex.split(f"gb2_ds_list --user {self.dirac_user} {self.gbasf2_project_name}")
        output_dataset_str = subprocess.run(ds_list_command, check=True, env=self.gbasf2_env, stdout=PIPE, encoding="utf-8").stdout
        if "No datasets" in output_dataset_str:
            raise RuntimeError(f"Not dataset to download under project name {self.gbasf2_project_name}")
        output_dataset_basenames = {os.path.basename(grid_path) for grid_path in output_dataset_str.splitlines()}

        # Define setting for directory, into which the output dataset should be
        # downloaded. The ``gb2_ds_get`` command will create in that a directory
        # with the name of the project, which will contain the root files.
        gbasf2_download_dir = get_setting("gbasf2_download_directory", default=".", task=self.task)
        dataset_dir = os.path.join(gbasf2_download_dir, self.gbasf2_project_name)

        # check if dataset had been already downloaded and if so, skip downloading
        if os.path.isdir(dataset_dir) and os.path.listdir(dataset_dir) == output_dataset_basenames:
            print("Dataset already exists, skipping download.")
            return

        # Go into temporary directory to download dataset. On success move it to the final destination
        os.makedirs(gbasf2_download_dir, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=gbasf2_download_dir) as tmpdirname:
            ds_get_command = shlex.split(f"gb2_ds_get --force --user {self.dirac_user} {self.gbasf2_project_name}")
            print("Downloading dataset with command ", " ".join(ds_get_command))
            output = subprocess.run(ds_get_command, check=True, env=self.gbasf2_env,
                                    stdout=PIPE, encoding="utf-8", cwd=tmpdirname).stdout
            print(output)
            if "No file found" in output:
                raise RuntimeError(f"No output data for gbasf2 project {self.gbasf2_project_name} found.")

            temporary_dataset_dir = os.path.join(tmpdirname, self.gbasf2_project_name)
            downloaded_dataset_basenames = set(os.listdir(temporary_dataset_dir))
            if output_dataset_basenames == downloaded_dataset_basenames:
                shutil.move(src=temporary_dataset_dir, dst=os.path.join(gbasf2_download_dir, self.gbasf2_project_name))

        # TODO: in the output dataset there is a root file created for each
        # file in the input dataset.The output files are have numbers added to
        # the filenames specified by the file names e.g. in the ``RootOutput``
        # and ``VariablesToNtuple`` modules. That makes it hard for the user to
        # define the output requirements in the ``output`` method of his task.
        # So maybe merge the output files or do something else to facilitate
        # defining outputs and checking that job is complete.

    def _download_logs(self):
        """
        Download sandbox files from grid with logs for each job in the gbasf2 project.

        It wraps ``gb2_job_output``, which downloads the job sandbox, which has the following structure:

        .. code-block:: text

            log
            └── <project name>
                ├── <first job id>
                │   ├── job.info
                │   ├── Script1_basf2helper.py.log # basf2 outputs
                │   └── std.out
                ├── <second job id>
                │   ├── ...
                ...

        These are stored in the task log dir.
        """
        download_logs_command = shlex.split(f"gb2_job_output --user {self.dirac_user} -p {self.gbasf2_project_name}")
        subprocess.run(download_logs_command, check=True, cwd=self.log_file_dir, env=self.gbasf2_env)


def get_unique_gbasf2_project_name(task):
    """
    Combine the ``gbasf2_project_name_prefix`` setting and the ``task_id`` hash
    to a unique project name.

    This is done to make sure that different instances of a task with different
    luigi parameters result in different gbasf2 project names.  When trying to
    redoing a task on the grid with identical parameters, rename the project
    name prefix, to ensure that you get a new project.
    """
    try:
        gbasf2_project_name_prefix = get_setting("gbasf2_project_name_prefix", task=task)
    except AttributeError as err:
        raise Exception(
            "Task can only be used with the gbasf2 batch process if it has ``gbasf2_project_name_prefix`` " +
            "as a luigi parameter, attribute or setting."
        ) from err

    task_id_hash = task.task_id.split("_")[-1]
    gbasf2_project_name = gbasf2_project_name_prefix + task_id_hash
    max_project_name_length = 32
    assert len(gbasf2_project_name) <= max_project_name_length,\
        f"Maximum length of project name should be {max_project_name_length}, " + \
        f"but has {len(gbasf2_project_name)} chars." + \
        f"Please choose a gbasf2_project_name_prefix of less than {max_project_name_length - len(task_id_hash)} characters," + \
        f" since the unique task id hash takes {len(task_id_hash)} characters."
    assert gbasf2_project_name.isalnum(), "Only alphanumeric project names are officially supported by gbasf2"
    return gbasf2_project_name
