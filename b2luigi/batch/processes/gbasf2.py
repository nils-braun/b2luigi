import os
import shlex
import subprocess
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
    Batch process for working with gbasf2 projects on grid.

    A BatchProcess job corresponds to a whole project in gbasf2.  The task of
    creating and managing jobs in a project is left to gbasf2.

    .. hint::
       - The gbasf2 batch process for luigi can only be used for tasks
         inhereting from ``Basf2PathTask`` or other tasks with a
         ``create_path()`` method that returns a basf2 path.
       - It can only be used for pickable/serializable basf2 paths, as it stores
         the path created by ``create_path`` in a python pickle file and runs that on the grid.
       - That means that aliases are, at least of now, not supported

    Example file to execute analysis path created in
    ``examples/gbasf2/example_mdst_analysis`` on grid via gbasf2:

    ..  literalinclude:: ../../examples/gbasf2/gbasf2_example.py
        :caption: File: ``examples/gbasf2/gbasf2_example.py``
        :linenos:

    Some settings are done as task-specific class attributes, others are defined
    in the ``settings.json``:

    ..  literalinclude:: ../../examples/gbasf2/settings.json
        :caption: File: ``examples/gbasf2/settings.json``
        :linenos:
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        try:
            #: gbasf2 project name, must be property/attribute, e.g. a luigi parameter
            #  Setting it via a setting.json file is not supported to make sure users set unique project names
            self.gbasf2_project_name = self.task.gbasf2_project_name
        except AttributeError as err:
            raise Exception(
                "Task can only be used with the gbasf2 batch process if it has ``gbasf2_project_name`` " +
                "as a luigi parameter or attribute. Make sure that the project name is unique for different " +
                f"instances of ``{type(self.task).__name__}()`` with different parameters."
            ) from err

        assert len(self.gbasf2_project_name) <= 32,\
            f"Maximum lenght of project name should be 32 characters, has {len(self.gbasf2_project_name)} chars"
        assert self.gbasf2_project_name.isalnum(), "Only alphanumeric project names are officially supported by gbasf2"

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

    @property
    def gbasf2_env(self):
        """
        Return the gbasf2 environment dict.

        When first called, it executes the setup script from the
        ``gbasf2_install_directory`` setting and ``gb2_proxy_init -g belle``
        command.  Then, it stores the result in the ``self._cached_gbasf2_env``
        property and just returns that on subsequent calls, so that the user
        does not have provide his password each time this function is called.
        This property can be used as the ``env`` parameter in subprocess calls,
        to execute gbasf2 commands in this environment
        """

        if self._cached_gbasf2_env is None:
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
            gbasf2_setup_command_str = f"source {gbasf2_setup_path} > /dev/null && gb2_proxy_init -g belle > /dev/null"
            # command to execute the gbasf2 setup command in a fresh shell and output the produced environment
            echo_gbasf2_env_command = shlex.split(f"env -i bash -c '{gbasf2_setup_command_str} > /dev/null && env'")
            gbasf2_env_string = subprocess.run(echo_gbasf2_env_command, check=True, stdout=subprocess.PIPE, encoding="utf-8").stdout
            self._cached_gbasf2_env = dict(line.split("=", 1) for line in gbasf2_env_string.splitlines())
        return self._cached_gbasf2_env

    #: cached gbasf2 enviromnent, initiallized and accessed via ``self.gbasf2_env``
    _cached_gbasf2_env = None

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

        # print summary of jobs in project if setting is set and job status changed
        if (get_setting("gbasf2_print_project_status", default=True, task=self.task) and
                n_jobs_by_status != self._n_jobs_by_status):
            job_summary_string = " ".join(f"{status}: {njobs}," for status, njobs in n_jobs_by_status.items())
            print(f"Status of jobs in project {self.gbasf2_project_name}:", job_summary_string)
        self._n_jobs_by_status = n_jobs_by_status

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

        if n_done < n_jobs and n_waiting + n_being_processed + n_done == n_jobs:
            return JobStatus.running

        # Setting for the success requirement of a gbasf2 project, e.g. if all
        # sub-jobs need to be done or to allow for some failed jobs
        gbasf2_require_all_jobs_done = get_setting("gbasf2_require_all_jobs_done", default=True, task=self.task)

        if gbasf2_require_all_jobs_done:
            # Require all jobs to be done for project success, any job failure results in a failed project
            if n_done == n_jobs:
                # download dataset on job success
                self._job_on_success_action()
                return JobStatus.successful
            if n_failed > 0:
                self._download_logs()
                return JobStatus.aborted
        else:
            # in this case require allow for some failed jobs, return project
            # success if some jobs were successful and no jobs are running
            # anymore, even if some jobs in project failed, as long as not all failed.
            if n_done > 0 and n_being_processed + n_waiting == 0:
                self._job_on_success_action()
                return JobStatus.successful
            if n_failed == n_jobs:
                self._download_logs()
                return JobStatus.aborted
        # TODO reschedule failed jobs via ``gb2_job_reschedule``
        raise RuntimeError("Could not determine JobStatus")

    def _job_on_success_action(self):
        """
        Things to do after the job had been successful, e.g. downloading the dataset and logs
        """
        self._download_dataset()
        self._download_logs()

    def start_job(self):
        """Submit new gbasf2 project to grid"""
        if self._check_project_exists() and self.get_job_status() == JobStatus.running:
            # If project already has been submitted to grid and is running,
            # don't start new project, but wait for current one to finish and
            # download that. Just issue warning. If the job exists on the grid
            # but is Done/Successfull or Failed, continue to submit new job,
            # because then the user will have to deal with an interactive prompt
            # by gbasf2 that appears when submittinto an existing project.
            warnings.warn(f"Project {self.gbasf2_project_name} is already submitted to grid."
                          "Project is therefore not restarted, instead waiting for existing project to finish.")
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
        command = shlex.split(f"gb2_job_delete --force -p {self.gbasf2_project_name}")
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

        # TODO: support tasks which don't need input dataset
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
        force_submission = get_setting("gbasf2_force_submission", default=False, task=self.task)
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
        print(f"\nSaved serialized path in {self.pickle_file_path}\nwith content:\n")
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
        command = shlex.split(f"gb2_job_status -p {self.gbasf2_project_name}")
        output = subprocess.run(command, check=True, stdout=subprocess.PIPE, encoding="utf-8", env=self.gbasf2_env).stdout
        if output.strip() == "0 jobs are selected.":
            return False
        if "--- Summary of Selected Jobs ---" in output:
            return True
        raise RuntimeError("Output of gb2_job_status did not contain expected strings,"
                           " could not determine if project exists")

    def _get_n_jobs_by_status(self):
        """
        Returns a dictionary with different gbasf2 job status as keys and the number of jobs in each category as values.
        """
        assert self._check_project_exists(), f"Project {self.gbasf2_project_name} doest not exist yet"

        command = shlex.split(f"gb2_job_status -p {self.gbasf2_project_name}")
        output = subprocess.run(command, check=True, output=subprocess.PIPE, encoding="utf-8", env=self.gbasf2_env).stdout
        # get job summary dict in the form of e.g.
        # {'Completed': 0, 'Deleted': 0, 'Done': 255, 'Failed': 0,
        # 'Killed': 0, 'Running': 0, 'Stalled': 0, 'Waiting': 0}
        job_summary_string = output.splitlines()[-1].strip()
        n_jobs_by_status = dict((summary_substring.split(":", 1)[0].strip(),
                                 int(summary_substring.split(":", 1)[1].strip()))
                                for summary_substring in job_summary_string.split())
        status_keys = list(n_jobs_by_status.keys())
        status_keys.sort()
        assert status_keys == ['Completed', 'Deleted', 'Done', 'Failed',
                               'Killed', 'Running', 'Stalled', 'Waiting'],\
            "Error when obtaining job summary, it does not contain the required status keys"
        return n_jobs_by_status

    def _download_dataset(self):
        """Download the results from a gbasf2 project, stored as a dataset on the grid."""
        # Define setting for directory, into which the output dataset should be
        # downloaded. The ``gb2_ds_get`` command will create in that a directory
        # with the name of the project, which will contain the root files.
        gbasf2_download_dir = get_setting("gbasf2_download_directory", default=".", task=self.task)
        os.makedirs(gbasf2_download_dir, exist_ok=True)
        command = shlex.split(f"gb2_ds_get --force {self.gbasf2_project_name}")
        print("Downloading dataset with command ", " ".join(command))
        output = subprocess.run(command, check=True, env=self.gbasf2_env, output=subprocess.PIPE, encoding="utf-8", cwd=gbasf2_download_dir).stdout
        print(output)
        if "No file found" in output:
            raise RuntimeError(f"No output data for gbasf2 project {self.gbasf2_project_name} found.")
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
        download_logs_command = shlex.split(f"gb2_job_output -p {self.gbasf2_project_name}")
        subprocess.run(download_logs_command, check=True, cwd=self.log_file_dir, env=self.gbasf2_env)
