import hashlib
import json
import os
import re
import shlex
import shutil
import subprocess
import tempfile
import warnings
from collections import Counter
from collections.abc import Iterable
from datetime import datetime
from functools import lru_cache

from b2luigi.basf2_helper.utils import get_basf2_git_hash
from b2luigi.batch.processes import BatchProcess, JobStatus
from b2luigi.core.settings import get_setting
from b2luigi.core.utils import flatten_to_dict, get_log_file_dir, get_task_file_dir
from jinja2 import Template
from luigi.target import Target


class Gbasf2Process(BatchProcess):
    """
    Batch process for working with gbasf2 projects on the *LHC Computing Grid* (LCG).

    Features
        - **gbasf2 project submission**

          The gbasf2 batch process takes the basf2 path returned by the
          ``create_path()`` method of the task, saves it into a pickle file to
          the disk and creates a wrapper steering file that executes the saved
          path. Any basf2 variable aliases added in the ``create_path()`` method
          are also stored in the pickle file. It then sends both the pickle file
          and the steering file wrapper to the grid via the BelleII-specific
          Dirac-wrapper gbasf2.

        - **Project status monitoring**

          After the project submission, the gbasf batch process regularly checks the status
          of all the jobs belonging to a gbasf2 project returns a success if
          all jobs had been successful, while a single failed job results in a failed project.
          You can close a running b2luigi process and then start your script again and if a
          task with the same project name is running, this b2luigi gbasf2 wrapper will recognize that
          and instead of resubmitting a new project, continue monitoring the running project.

        - **Automatic download of datasets and logs**

          If all jobs had been successful, it automatically downloads the output dataset and
          the log files from the job sandboxes and automatically checks if the download was successful
          before moving the data to the final location. On failure, it only downloads the logs.
          The dataset download can be optionally disabled.

        - **Automatic rescheduling of failed jobs**

          Whenever a job fails, gbasf2 reschedules it as long as the number of retries is below the
          value of the setting ``gbasf2_max_retries``. It keeps track of the number of retries in a
          local file in the ``log_file_dir``, so that it does not change if you close b2luigi and start it again.
          Of course it does not persist if you remove that file or move to a different machine.

    .. note::
       Despite all the automatization that this gbasf2 wrapper provides, the user is expected to
       have a basic understanding of how the grid works and know the basics of working
       with gbasf2 commands manually.

    Caveats
        - The gbasf2 batch process for luigi can only be used for tasks
          inhereting from ``Basf2PathTask`` or other tasks with a
          ``create_path()`` method that returns a basf2 path.

        - It can be used **only for pickable basf2 paths**, as it stores
          the path created by ``create_path`` in a python pickle file and runs that on the grid.
          Therefore, **python basf2 modules are not yet supported**.
          To see if the path produced by a steering file is pickable, you can try to dump it with
          ``basf2 --dump-path`` and execute it again with ``basf2 --execute-path``.

        - Output format: Changing the batch to gbasf2 means you also have to
          adapt how you handle the output of your gbasf2 task in tasks depending
          on it, because the output will not be a single root file anymore (e.g.
          ``B_ntuple.root``), but a collection of root files, one for each file in
          the input data set, in a directory with the base name of the root
          files, e.g.::
            <task output directory>
                        ├── B_ntuple.root
                        │   └── B_ntuple_0.root
                        │   └── B_ntuple_1.root
                        │   └── ...
                        ├── D_ntuple.root
                        │   └── D_ntuple_0.root
                        │   └── ...



    Settings for gbasf2 tasks
        To submit a task with the gbasf2 wrapper, you first you have to add the property
        ``batch_system = "gbasf2"``, which sets the ``batch_system`` setting.
        It is not recommended to set that setting globally, as not all tasks can be submitted to the grid,
        but only tasks with a ``create_path`` method.

        For gbasf2 tasks it is further required to set the settings

        - ``gbasf2_input_dataset``: String with the logical path of a dataset on the grid to use as an input to the task.
          You can provide multiple inputs by having multiple paths contained in this string, separated by commas without spaces.
          An alternative is to just instantiate multiple tasks with different input datasets, if you want to know in retrospect
          which input dataset had been used for the production of a specific output.
        - ``gbasf2_project_name_prefix``: A string with which your gbasf2 project names will start.
          To ensure the project associate with each unique task (i.e. for each of luigi parameters)
          is unique, the unique ``task.task_id`` is hashed and appended to the prefix
          to create the actual gbasf2 project name.
          Should be below 22 characters so that the project name with the hash can remain
          under 32 characters.

        The following example shows a minimal class with all required options to run on the gbasf2/grid batch:

        .. code-block:: python

           class MyTask(Basf2PathTask):
               batch_system = "gbasf2"
               gbasf2_project_name_prefix = b2luigi.Parameter(significant=False)
               gbasf2_input_dataset = b2luigi.Parameter(hashed=True)

        Other not required, but noteworthy settings are:

        - ``gbasf2_install_directory``: Defaults to ``~/gbasf2KEK``. If you installed gbasf2 into another
          location, you have to change that setting accordingly.
        - ``gbasf2_release``: Defaults to the release of your currently set up basf2 release.
          Set this if you want the jobs to use another release on the grid.
        - ``gbasf2_print_status_updates``: Defaults to ``True``. By setting it to ``False`` you can turn off the
          printing of of the job summaries, that is the number of jobs in different states in a gbasf2 project.
        - ``gbasf2_max_retries``: Default to 0. Maximum number of times that each job in the project can be automatically
          rescheduled until the project is declared as failed.
        - ``gbasf2_download_dataset``: Defaults to ``True``. Disable this setting if you don't want to download the
          output dataset from the grid on job success. As you can't use the downloaded dataset as an output target for luigi,
          you should then use the provided ``Gbasf2GridProjectTarget``, as shown in the following example:

          .. code-block:: python

            from b2luigi.batch.processes.gbasf2 import get_unique_project_name, Gbasf2GridProjectTarget

            class MyTask(Basf2PathTask):
                # [...]
                def output(self):
                    project_name = get_unique_project_name(self)
                    return Gbasf2GridProjectTarget(project_name, task=self)

          This is useful when chaining gbasf2 tasks together,
          as they don't need the output locally but take the grid datasets as input. Also useful when you just want
          to produce data on the grid for other people to use.

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

       .. code-block:: python

          b2luigi.set_setting("gbasf2_additional_params",  "--banned_site LCG.KEK.jp")


    Example
        Here is an example file to submit an analysis path created by the script in
        ``examples/gbasf2/example_mdst_analysis`` to grid via gbasf2:

        .. literalinclude:: ../../examples/gbasf2/gbasf2_example.py
           :caption: File: ``examples/gbasf2/gbasf2_example.py``
           :linenos:

    Handling failed jobs
        The gbasf2 input wrapper considers the gbasf2 project as failed if any of
        the jobs in the project failed and reached the maximum number of retries.
        It then automatically downloads the logs, so please look into them to see what the reason was.
        For example, it can be that only certain grid sites were affected, so you might want to exclude them
        by adding the ``"--banned_site ...`` to ``gbasf2_additional_params``.

        You also always reschedule jobs manually with the ``gb2_job_reschedule`` command or delete them with
        ``gb2_job_delete`` so that the gbasf2 batch process doesn't know they ever
        existed. Then run just run your luigi task/script again and it will start monitoring the running project
        again.
    """

    # directory of the file in which this class is defined
    _file_dir = os.path.dirname(os.path.realpath(__file__))

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        #: gbasf2 project name, must be property/attribute, e.g. a luigi parameter
        # Setting it via a setting.json file is not supported to make sure users set unique project names
        self.gbasf2_project_name = get_unique_project_name(self.task)

        #: Output file directory of the task to wrap with gbasf2, where we will
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

        self.dirac_user = get_dirac_user()
        #: Maximum number of times that each job in the project can be rescheduled until the project is declared as failed.
        self.max_retries = get_setting("gbasf2_max_retries", default=0, task=self.task)

        #: Store number of times each job had been rescheduled
        self.n_retries_by_job = Counter()

        #: Local storage for ``n_retries_by_job`` counter
        # so that it persists even if luigi process is killed and restarted.
        self.retries_file_path = os.path.join(self.log_file_dir, "n_retries_by_grid_job.json")
        if os.path.isfile(self.retries_file_path):
            with open(self.retries_file_path, "r") as retries_file:
                retries_from_file = json.load(retries_file)
                self.n_retries_by_job.update(retries_from_file)

        # Store dictionary with n_jobs_by_status in attribute to check if it changed,
        # useful for printing job status on change only
        self._n_jobs_by_status = ""

        # Store whether the job had already been successful in a variable b/c
        # there's actions we want to do only the first time that
        # ``get_job_status`` returns a success.
        self._project_had_been_successful = False

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

        job_status_dict = get_gbasf2_project_job_status_dict(self.gbasf2_project_name, dirac_user=self.dirac_user)
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

        # The gbasf2 project is considered as failed if any of the jobs in it failed.
        # However, we first try to reschedule thos jobs and only declare it as failed if the maximum number of retries
        # for reschedulinhas been reached
        if n_failed > 0:
            self._on_failure_action()
            if self.max_retries > 0 and self._reschedule_failed_jobs():
                return JobStatus.running
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
        self._download_logs()
        if get_setting("gbasf2_download_dataset", default=True, task=self.task):
            self._download_dataset()

    def _on_failure_action(self):
        """
        Things to do after the project failed
        """
        job_status_dict = get_gbasf2_project_job_status_dict(self.gbasf2_project_name, dirac_user=self.dirac_user)
        failed_job_dict = {job_id: job_info for job_id, job_info in job_status_dict.items()
                           if job_info["Status"] == "Failed"}
        n_failed = len(failed_job_dict)
        print(f"{n_failed} failed jobs:\n{failed_job_dict}")
        self._download_logs()

    def _reschedule_failed_jobs(self):
        """
        Tries to reschedule failed jobs in the project if ``self.max_retries`` has not been reached
        and returns ``True`` if rescheduling has been successful.
        """
        job_status_dict = get_gbasf2_project_job_status_dict(
            self.gbasf2_project_name, dirac_user=self.dirac_user)
        for job_id, job_info in job_status_dict.items():
            if job_info["Status"] == "Failed":
                if self.n_retries_by_job[job_id] >= self.max_retries:
                    warnings.warn(f"Reached maximum number of rescheduling tries ({self.max_retries}) for job {job_id}.")
                    return False
                self._reschedule_job(job_id)
                self.n_retries_by_job[job_id] += 1
                with open(self.retries_file_path, "w") as retries_file:
                    json.dump(self.n_retries_by_job, retries_file)
        return True

    def _reschedule_job(self, job_id):
        """
        Reschedule job if the number of retries for it is below ``self.max_retries``
        """
        n_retries = self.n_retries_by_job[job_id]
        print(f"Rescheduling job {job_id} (retry no. {n_retries + 1}).")
        if self.n_retries_by_job[job_id] < self.max_retries:
            reschedule_command = shlex.split(f"gb2_job_reschedule --jobid {job_id} --force")
            run_with_gbasf2(reschedule_command)

    def start_job(self):
        """
        Submit new gbasf2 project to grid
        """
        if check_project_exists(self.gbasf2_project_name, self.dirac_user):
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
        print(f"\nSubmitting gbasf2 project: {self.gbasf2_project_name}")
        print("\nUsing command:\n" + " ".join(gbasf2_command) + "\n")

        # Symlink the pickle_file from the ``task_file_dir`` (where it is store
        # for reproducibility) to the current working directory, so that gbasf2
        # copies it into the same directory in the grid input sandbox as the
        # steering file.
        pickle_file_symlink_destination = os.path.basename(self.pickle_file_path)
        try:
            os.symlink(self.pickle_file_path, pickle_file_symlink_destination, target_is_directory=False)
            run_with_gbasf2(gbasf2_command)
        finally:
            os.unlink(pickle_file_symlink_destination)

    def kill_job(self):
        """
        Kill gbasf2 project
        """
        if not check_project_exists(self.gbasf2_project_name, dirac_user=self.dirac_user):
            return
        # Note: The two commands ``gb2_job_delete`` and ``gb2_job_kill`` differ
        # in that deleted jobs are killed and removed from the job database,
        # while only killed jobs can be restarted.
        command = shlex.split(f"gb2_job_kill --force --user {self.dirac_user} -p {self.gbasf2_project_name}")
        run_with_gbasf2(command)

    def _build_gbasf2_submit_command(self):
        """
        Function to create the gbasf2 submit command to pass to run_with_gbasf2
        from the task options and attributes.
        """
        gbasf2_release = get_setting("gbasf2_release", default=get_basf2_git_hash(), task=self.task)
        gbasf2_additional_files = get_setting("gbasf2_additional_files", default=[], task=self.task)
        if not isinstance(gbasf2_additional_files, Iterable) or isinstance(gbasf2_additional_files, str):
            raise ValueError("``gbasf2_additional_files`` is not an iterable or strings.")
        gbasf2_input_sandbox_files = [os.path.basename(self.pickle_file_path)] + list(gbasf2_additional_files)
        gbasf2_command_str = (f"gbasf2 {self.wrapper_file_path} -f {' '.join(gbasf2_input_sandbox_files)} " +
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
            if not (0 <= priority <= 10):
                raise ValueError("Priority should be integer between 0 and 10.")
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
        if gbasf2_additional_params is not False:
            gbasf2_command_str += f" {gbasf2_additional_params} "

        gbasf2_command = shlex.split(gbasf2_command_str)
        return gbasf2_command

    def _write_path_to_file(self):
        """
        Serialize and save the ``basf2.Path`` returned by ``self.task.create_path()`` to a python pickle file.

        Also serializes any basf2 variable aliases from the current variable manager instance.
        """
        try:
            basf2_path = self.task.create_path()
        except AttributeError as err:
            raise Exception(
                "Gbasf2 batch process can only used with tasks that generate basf2 paths with "
                "a ``create_path()`` method, e.g. are an instance of ``Basf2PathTask``."
            ) from err
        from b2luigi.batch.processes.gbasf2_utils.pickle_utils import write_path_and_aliases_to_file
        write_path_and_aliases_to_file(basf2_path, self.pickle_file_path)

    def _create_wrapper_steering_file(self):
        """
        Create a steering file to send to the grid that executes the pickled
        basf2 path from ``self.task.create_path()``.
        """
        # read a jinja2 template for the steerinfile that should execute the pickled path
        template_file_path = os.path.join(self._file_dir, "templates/gbasf2_steering_file_wrapper.jinja2")
        with open(template_file_path, "r") as template_file:
            template = Template(template_file.read())
            # replace some variable values in the templates
            steering_file_stream = template.stream(
                pickle_file_path=os.path.basename(self.pickle_file_path),
                max_event=get_setting("max_event", default=0, task=self.task),
            )
            # write the template with the replacements to a new file which should be sent to the grid
            steering_file_stream.dump(self.wrapper_file_path)

    def _download_dataset(self):
        """
        Download the task outputs from the gbasf2 project dataset.

        For each task output defined via ``self.add_to_output(<name>.root)`` a
        directory will be created, into which all files named ``name_*.root`` on
        the grid dataset corresponding to the project name will be downloaded.
        The download is ensured to be automatic by first downloading into
        temporary directories.
        """
        if not check_dataset_exists_on_grid(self.gbasf2_project_name, dirac_user=self.dirac_user):
            raise RuntimeError(f"Not dataset to download under project name {self.gbasf2_project_name}")
        task_output_dict = flatten_to_dict(self.task.output())
        for output_file_name, output_target in task_output_dict.items():
            output_dir_path = output_target.path
            assert output_file_name == os.path.basename(output_file_name)  # not sure I need this
            output_file_stem, output_file_ext = os.path.splitext(output_file_name)
            if not output_file_ext == ".root":
                raise ValueError(
                    f"Output file name \"{output_file_name}\" does not end with \".root\", "
                    "but gbasf2 batch only supports root outputs"
                )

            # Get list of files that we want to download from the grid via ``gb2_ds_list`` so that we can
            # then compare this list with the results of the download to see if it was successful
            dataset_query_string = \
                f"/belle/user/{self.dirac_user}/{self.gbasf2_project_name}/sub00/{output_file_stem}_*{output_file_ext}"
            ds_list_command = shlex.split(f"gb2_ds_list {dataset_query_string}")
            output_dataset_grid_filepaths = run_with_gbasf2(ds_list_command, capture_output=True).stdout.splitlines()
            output_dataset_basenames = {os.path.basename(grid_path) for grid_path in output_dataset_grid_filepaths}
            # check if dataset had been already downloaded and if so, skip downloading
            if os.path.isdir(output_dir_path) and os.listdir(output_dir_path) == output_dataset_basenames:
                print(f"Dataset already exists in {output_dir_path}, skipping download.")
                return

            # To prevent from task being accidentally marked as complete when the gbasf2 dataset download failed,
            # we create a temporary directory in the parent of ``output_dir_path`` and first download the dataset there.
            # The download command will download it into a subdirectory with the same name as the project.
            # If the download had been successful and the local files are identical to the list of files on the grid,
            # we move the downloaded dataset to the location specified by ``output_dir_path``
            output_dir_parent = os.path.dirname(output_dir_path)
            os.makedirs(output_dir_parent, exist_ok=True)
            with tempfile.TemporaryDirectory(dir=output_dir_parent) as tmpdir_path:
                ds_get_command = shlex.split(f"gb2_ds_get --force {dataset_query_string}")
                print("Downloading dataset with command ", " ".join(ds_get_command))
                stdout = run_with_gbasf2(ds_get_command, cwd=tmpdir_path, capture_output=True).stdout
                print(stdout)
                if "No file found" in stdout:
                    raise RuntimeError(f"No output data for gbasf2 project {self.gbasf2_project_name} found.")
                tmp_output_dir = os.path.join(tmpdir_path, self.gbasf2_project_name, 'sub00')
                downloaded_dataset_basenames = set(os.listdir(tmp_output_dir))
                if output_dataset_basenames == downloaded_dataset_basenames:
                    print(f"Download of {self.gbasf2_project_name} files successful.\n"
                          f"Moving output files to directory: {output_dir_path}")
                    if os.path.exists(output_dir_path):
                        shutil.rmtree(output_dir_path)
                    shutil.move(src=tmp_output_dir, dst=output_dir_path)
                else:
                    raise RuntimeError(
                        f"The downloaded set of files in {tmp_output_dir} is not equal to the " +
                        f"list of dataset files on the grid for project {self.gbasf2_project_name}." +
                        "\nDownloaded files:\n{}".format("\n".join(downloaded_dataset_basenames)) +
                        "\nFiles on the grid:\n{}".format("\n".join(output_dataset_basenames))
                    )

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
        # We want to overwrite existing logs when retrieving the logs multiple
        # times. To avoid a situation where we delete the existing logs before a
        # new log download, but then the download fails and the user might be
        # left with no logs, first the log download is performed to a temporary
        # directory and then moved to the final location
        with tempfile.TemporaryDirectory(dir=self.log_file_dir) as tmpdir_path:
            download_logs_command = shlex.split(f"gb2_job_output --user {self.dirac_user} -p {self.gbasf2_project_name}")
            run_with_gbasf2(download_logs_command, cwd=tmpdir_path)
            tmp_gbasf2_log_path = os.path.join(tmpdir_path, "log", self.gbasf2_project_name)
            gbasf2_project_log_dir = os.path.join(self.log_file_dir, "gbasf2_logs", self.gbasf2_project_name)
            print(f"Download of logs for gbasf2 project {self.gbasf2_project_name} successful.\n"
                  f" Moving logs to {gbasf2_project_log_dir}")
            if os.path.exists(gbasf2_project_log_dir):
                shutil.rmtree(gbasf2_project_log_dir)
            shutil.move(tmp_gbasf2_log_path, gbasf2_project_log_dir)


class Gbasf2GridProjectTarget(Target):
    """
    Target exists if an output dataset for the project exists on the grid and is
    not being written to, i.e. all jobs that produced the dataset are done.
    """

    def __init__(self, project_name, dirac_user=None):
        """
        :param project_name: Name of the gbasf2 grid project that produced the
            dataset and under which the dataset is stored
        :param dirac_user: Dirac user, who produced the output dataset.  When
            ``None``, the current user is used.
        """
        self.project_name = project_name
        self.dirac_user = dirac_user

    def exists(self):
        if not check_dataset_exists_on_grid(self.project_name, dirac_user=self.dirac_user):
            # there's no dataset associated with that name on the grid
            return False
        if check_project_exists(self.project_name, dirac_user=self.dirac_user):
            # if there's data named after that project on the grid, ensure there are no jobs writig to it
            project_status_dict = get_gbasf2_project_job_status_dict(self.project_name, self.dirac_user)
            all_jobs_done = all(job_info["Status"] == "Done" for job_info in project_status_dict.values())
            if not all_jobs_done:
                return False
        return True


def check_dataset_exists_on_grid(gbasf2_project_name, dirac_user=None):
    """
    Use ``gb2_ds_list`` command to see if an output dataset exists for the gbasf2 project
    """
    if dirac_user is None:
        dirac_user = get_dirac_user()
    ds_list_command = shlex.split(f"gb2_ds_list --user {dirac_user} {gbasf2_project_name}")
    output_dataset_str = run_with_gbasf2(ds_list_command, capture_output=True).stdout
    if "No datasets" in output_dataset_str:
        return False
    output_lines_are_paths = all(os.path.abspath(line) for line in output_dataset_str.strip().splitlines())
    if not output_lines_are_paths:
        warnings.warn("The output of ``{' '.join(ds_list_command)}`` contains lines that are not grid paths:\n" +
                      output_dataset_str)
        return False
    return True


def get_gbasf2_project_job_status_dict(gbasf2_project_name, dirac_user=None):
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
    if dirac_user is None:
        dirac_user = get_dirac_user()
    job_status_script_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                          "gbasf2_utils/gbasf2_job_status.py")
    job_status_command = shlex.split(f"{job_status_script_path} -p {gbasf2_project_name} --user {dirac_user}")
    proc = run_with_gbasf2(job_status_command, capture_output=True, check=False)
    # FIXME: use enum or similar to define my own return codes
    if proc.returncode == 3:  # return code 3 means project does not exist yet
        raise RuntimeError(
            f"\nCould not find any jobs for project {gbasf2_project_name} on the grid.\n" +
            "Probably there was an error during the project submission when running the gbasf2 command.\n"
        )
    job_status_json_string = proc.stdout
    job_status_dict = json.loads(job_status_json_string)
    return job_status_dict


def check_project_exists(gbasf2_project_name, dirac_user=None):
    """
    Check if we can find the gbasf2 project on the grid with ``gb2_job_status``.
    """
    try:
        return bool(get_gbasf2_project_job_status_dict(gbasf2_project_name, dirac_user))
    except RuntimeError:
        return False


def run_with_gbasf2(
        cmd, *args, ensure_proxy_initialized=True, check=True, encoding="utf-8", capture_output=False, **kwargs
):
    """
    Call ``cmd`` in a subprocess with the gbasf2 environment.

    :param ensure_proxy_initialized: If this is True, check if the dirac proxy is initalized and alive and if not,
                                     initialize it.
    :param check: Whether to raise a ``CalledProcessError`` when the command returns with an error code.
                  The default value ``True`` is the same as in ``subprocess.check_call()`` and different as in the
                  normal ``run_with_gbasf2()`` command.
    :param capture_output: Whether to capture the ``stdout`` and ``stdin``.
                           Same as setting them to in ``subprocess.PIPE``.
                           The implementation of this argument was taken from the ``subprocess.run()`` in python3.8.
    :param encoding: Encoding to use for the interpretation of the command output.
                     Different from normal subprocess commands, it by default assumes "utf-8". In that case, the returned
                     ``stdout`` and ``stderr`` are strings and not byte-strings and the user doesn't have to decode them
                     manually.
    :return: ``CompletedProcess`` instance
    """
    if capture_output:
        if kwargs.get('stdout') is not None or kwargs.get('stderr') is not None:
            raise ValueError('stdout and stderr arguments may not be used '
                             'with capture_output.')
        kwargs['stdout'] = subprocess.PIPE
        kwargs['stderr'] = subprocess.PIPE
    gbasf2_env = get_gbasf2_env()
    if ensure_proxy_initialized:
        setup_dirac_proxy()
    proc = subprocess.run(cmd, *args, check=check, encoding=encoding, env=gbasf2_env, **kwargs)
    return proc


@lru_cache(maxsize=None)
def get_gbasf2_env(gbasf2_install_directory=None):
    """
    Return the gbasf2 environment dict which can be used to run gbasf2 commands.

    :param gbasf2_install_directory: Directory into which gbasf2 has been
        installed.  When set to the default value ``None``, it looks for the
        value of the ``gbasf2_install_directory`` setting and when that is not
        set, it uses the default of most installation instructions, which is
        ``~/gbasf2KEK``.
    :return: Dictionary containing the  environment that you get from sourcing the gbasf2 setup script.
    """
    if gbasf2_install_directory is None:
        gbasf2_install_directory = get_setting("gbasf2_install_directory", default="~/gbasf2KEK")
    gbasf2_setup_path = os.path.join(gbasf2_install_directory, "BelleDIRAC/gbasf2/tools/setup")
    if not os.path.isfile(os.path.expanduser(gbasf2_setup_path)):
        raise FileNotFoundError(
            f"Could not find gbasf2 setup files in ``{gbasf2_install_directory}``.\n" +
            "Make sure to that gbasf2 is installed at that location."
        )
    # complete bash command to set up the gbasf2 environment
    # piping output to /dev/null, because we want that our final script only prints the ``env`` output
    gbasf2_setup_command_str = f"source {gbasf2_setup_path} > /dev/null"
    # command to execute the gbasf2 setup command in a fresh shell and output the produced environment
    echo_gbasf2_env_command = shlex.split(f"env -i bash -c '{gbasf2_setup_command_str} > /dev/null && env'")
    gbasf2_env_string = subprocess.run(
        echo_gbasf2_env_command, check=True, stdout=subprocess.PIPE, encoding="utf-8"
    ).stdout
    gbasf2_env = dict(line.split("=", 1) for line in gbasf2_env_string.splitlines())
    return gbasf2_env


def get_dirac_user():
    """Get dirac user name"""
    proxy_info_str = run_with_gbasf2(["gb2_proxy_info"], capture_output=True).stdout
    for line in proxy_info_str.splitlines():
        if line.startswith("username"):
            dirac_user = line.split(":", 1)[1].strip()
            return dirac_user
    raise RuntimeError("Could not obtain dirac user name from `gb2_proxy_init` output.")


def setup_dirac_proxy():
    """
    Runs ``gb2_proxy_init -g belle`` if there's no active dirac proxy. If there is, do nothing.
    """
    check_proxy_initizalized_script_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        "gbasf2_utils/check_if_dirac_proxy_is_initialized.py"
    )
    # first run script to check if proxy is already alive or needs to be initalized
    # setting ``initalize_proxy=False`` is vital here, otherwise we get an infinite loop
    proc = run_with_gbasf2([check_proxy_initizalized_script_path], ensure_proxy_initialized=False, check=False)
    # if returncode of the script is 0, that means that proxy is already alive
    if not proc.returncode:
        return
    # initiallize proxy
    run_with_gbasf2(shlex.split("gb2_proxy_init -g belle"), ensure_proxy_initialized=False)


def get_unique_project_name(task):
    """
    Combine the ``gbasf2_project_name_prefix`` setting and the ``task_id`` hash
    to a unique project name.

    This is done to make sure that different instances of a task with different
    luigi parameters result in different gbasf2 project names. When trying to
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
    # luigi interally assings a hash to a task by calling the builtin ``hash(task.task_id)``,
    # but that returns a signed integer. I prefer a hex string to get more information per character,
    # which is why I decided to use ``hashlib.md5``.
    task_id_hash = hashlib.md5(task.task_id.encode()).hexdigest()[0:10]
    gbasf2_project_name = gbasf2_project_name_prefix + task_id_hash
    max_project_name_length = 32
    if len(gbasf2_project_name) > max_project_name_length:
        raise ValueError(
            f"Maximum length of project name should be {max_project_name_length}, "
            f"but has {len(gbasf2_project_name)} chars."
            "Please choose a gbasf2_project_name_prefix of less than "
            f"{max_project_name_length - len(task_id_hash)} characters,"
            f" since the unique task id hash takes {len(task_id_hash)} characters."
        )
    # Only alphanumeric characters (letters, numbers and `_`, `-`) are supported by gbasf2
    valid_project_name_regex_str = r"^[a-zA-Z0-9_-]*$"
    if not re.match(valid_project_name_regex_str, gbasf2_project_name):
        raise ValueError(
            f"Project name \"{gbasf2_project_name}\" is invalid. "
            "Only alphanumeric project names are officially supported by gbasf2."
        )
    return gbasf2_project_name
