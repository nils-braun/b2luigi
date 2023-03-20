import errno
import hashlib
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import warnings
from collections import Counter
from datetime import datetime
from functools import lru_cache
from getpass import getpass
from glob import glob
from itertools import groupby
from typing import Iterable, List, Optional, Set, Tuple

from b2luigi.basf2_helper.utils import get_basf2_git_hash
from b2luigi.batch.processes import BatchProcess, JobStatus
from b2luigi.core.settings import get_setting
from b2luigi.core.utils import (flatten_to_dict, get_log_file_dir,
                                get_task_file_dir)
from jinja2 import Template
from luigi.target import Target
from retry import retry


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

          .. hint::
            The outputs of gbasf2 tasks can be a bit overwhelming, so I recommend using the
            :ref:`central scheduler <central-scheduler-label>`
            which provides a nice overview of all tasks in the browser, including a status/progress
            indicator how many jobs in a gbasf2 project are already done.

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

        - It can be used **only for pickable basf2 paths**, with only some limited global basf2 state
          saved (currently aliases and global tags). The batch process stores
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
        - ``gbasf2_input_dslist``: Alternatively to ``gbasf2_input_dataset``, you can use this setting to provide a text file
          containing the logical grid path names, one per line.
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

        - ``gbasf2_install_directory``: Directory where gbasf2 is installed.
            Defaults to ``/cvmfs/belle.kek.jp/grid/gbasf2/pro/../..``, which should point to the latest gbasf2 release.
        - ``gbasf2_release``: Defaults to the release of your currently set up basf2 release.
          Set this if you want the jobs to use another release on the grid.
        - ``gbasf2_proxy_lifetime``: Defaults to 24. When initializing a proxy, set the
          lifetime to this number of hours.
        - ``gbasf2_min_proxy_lifetime``: Defaults to 0. During processing, prompt user
          to reinitialize proxy if remaining proxy lifetime drops below this number of
          hours.
        - ``gbasf2_print_status_updates``: Defaults to ``True``. By setting it to ``False`` you can turn off the
          printing of of the job summaries, that is the number of jobs in different states in a gbasf2 project.
        - ``gbasf2_max_retries``: Default to 0. Maximum number of times that each job in the project can be automatically
          rescheduled until the project is declared as failed.
        - ``gbasf2_proxy_group``: Default to ``"belle"``. If provided, the gbasf2 wrapper will work with the custom gbasf2 group,
          specified in this parameter. No need to specify this parameter in case of usual physics analysis at Belle II.
          If specified, one has to provide ``gbasf2_project_lpn_path`` parameter.
        - ``gbasf2_project_lpn_path``: Path to the LPN folder for a specified gbasf2 group.
          The parameter has no effect unless the ``gbasf2_proxy_group`` is used with non-default value.
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

        - ``gbasf2_download_logs``: Whether to automatically download the log output of gbasf2 projects when the
          task succeeds or fails. Having the logs is important for reproducibility, but k

        The following optional settings correspond to the equally named ``gbasf`` command line options
        (without the ``gbasf_`` prefix) that you can set to customize your gbasf2 project:

        ``gbasf2_noscout``,
        ``gbasf2_additional_files``,
        ``gbasf2_input_datafiles``,
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
        self.retries_file_path = os.path.join(
            self.log_file_dir, "n_retries_by_grid_job.json"
        )
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
        """
        job_status_dict = get_gbasf2_project_job_status_dict(
            self.gbasf2_project_name, dirac_user=self.dirac_user
        )
        n_jobs_by_status = Counter()
        for _, job_info in job_status_dict.items():
            n_jobs_by_status[job_info["Status"]] += 1

        # recheck the status more closely, and correct
        # reason: sometimes 'Status' marked as 'Done',
        # while 'ApplicationStatus' is not 'Done'
        for _, job_info in job_status_dict.items():
            if job_info["Status"] == "Done" and (
                job_info["ApplicationStatus"] != "Done"
            ):
                n_jobs_by_status["Done"] -= 1
                n_jobs_by_status["Failed"] += 1

        # print summary of jobs in project if setting is set and job status changed
        if (
            get_setting("gbasf2_print_status_updates", default=True, task=self.task)
            and n_jobs_by_status != self._n_jobs_by_status
        ):
            time_string = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            job_status_string = str(dict(sorted(n_jobs_by_status.items()))).strip("{}")
            print(
                f'Jobs in gbasf2 project "{self.gbasf2_project_name}" at {time_string}: {job_status_string}'
            )
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

        # task is running
        if n_in_final_state < n_jobs:
            # try updating progressbar for central scheduler web view
            percentage = n_done / n_jobs * 100
            self._scheduler.set_task_progress_percentage(self.task.task_id, percentage)

            status_message = "\n".join(
                f"{status}: {n_jobs}" for status, n_jobs in n_jobs_by_status.items()
            )
            self._scheduler.set_task_status_message(self.task.task_id, status_message)

            return JobStatus.running

        # Require all jobs to be done for project success, any job failure results in a failed project
        if n_done == n_jobs:
            # download dataset only the first time that we return JobStatus.successful
            if not self._project_had_been_successful:
                try:
                    self._on_first_success_action()
                    self._project_had_been_successful = True
                # RuntimeError might occur when download of output dataset was not complete. This is
                # frequent, so we want to catch that error and just marking the job as failed
                except RuntimeError as err:
                    warnings.warn(repr(err), RuntimeWarning)
                    return JobStatus.aborted

            return JobStatus.successful

        raise RuntimeError("Could not determine JobStatus")

    def _on_first_success_action(self):
        """
        Things to do after all jobs in the project had been successful, e.g. downloading the dataset and logs
        """
        if get_setting("gbasf2_download_logs", default=True, task=self.task):
            self._download_logs()
        if get_setting("gbasf2_download_dataset", default=True, task=self.task):
            self._download_dataset()

    def _on_failure_action(self):
        """
        Things to do after the project failed
        """
        job_status_dict = get_gbasf2_project_job_status_dict(
            self.gbasf2_project_name, dirac_user=self.dirac_user
        )
        failed_job_dict = {
            job_id: job_info
            for job_id, job_info in job_status_dict.items()
            if job_info["Status"] == "Failed"
            or (
                job_info["Status"] == "Done" and job_info["ApplicationStatus"] != "Done"
            )
        }
        n_failed = len(failed_job_dict)
        print(f"{n_failed} failed jobs:\n{failed_job_dict}")
        if get_setting("gbasf2_download_logs", default=True, task=self.task):
            self._download_logs()

    def _reschedule_failed_jobs(self):
        """
        Tries to reschedule failed jobs in the project if ``self.max_retries`` has not been reached
        and returns ``True`` if rescheduling has been successful.
        """
        jobs_to_be_rescheduled = []
        jobs_hitting_max_n_retries = []
        job_status_dict = get_gbasf2_project_job_status_dict(
            self.gbasf2_project_name, dirac_user=self.dirac_user
        )

        for job_id, job_info in job_status_dict.items():
            if job_info["Status"] == "Failed" or (
                job_info["Status"] == "Done" and job_info["ApplicationStatus"] != "Done"
            ):
                if self.n_retries_by_job[job_id] < self.max_retries:
                    self.n_retries_by_job[job_id] += 1
                    jobs_to_be_rescheduled.append(job_id)
                else:
                    jobs_hitting_max_n_retries.append(job_id)

        if jobs_to_be_rescheduled:
            self._reschedule_jobs(jobs_to_be_rescheduled)
            with open(self.retries_file_path, "w") as retries_file:
                json.dump(self.n_retries_by_job, retries_file)

        if jobs_hitting_max_n_retries:
            warnings.warn(
                f"Reached maximum number of rescheduling tries ({self.max_retries}) for following jobs:\n\t"
                + "\n\t".join(str(j) for j in jobs_hitting_max_n_retries)
                + "\n",
                RuntimeWarning,
            )
            return False

        return True

    def _reschedule_jobs(self, job_ids):
        """
        Reschedule chosen list of jobs.
        """
        print("Rescheduling jobs:")
        print(
            "\t"
            + "\n\t".join(
                f"{job_id} ({self.n_retries_by_job[job_id]} retries)"
                for job_id in job_ids
            )
        )

        reschedule_command = shlex.split(
            f"gb2_job_reschedule --jobid {' '.join(job_ids)} --force"
        )
        run_with_gbasf2(reschedule_command)

    def start_job(self):
        """
        Submit new gbasf2 project to grid
        """
        if check_project_exists(self.gbasf2_project_name, self.dirac_user):
            print(
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
        if os.path.islink(pickle_file_symlink_destination):
            os.remove(pickle_file_symlink_destination)
        try:
            os.symlink(
                self.pickle_file_path,
                pickle_file_symlink_destination,
                target_is_directory=False,
            )
            run_with_gbasf2(gbasf2_command)
        finally:
            os.unlink(pickle_file_symlink_destination)

    def kill_job(self):
        """
        Kill gbasf2 project
        """
        if not check_project_exists(
            self.gbasf2_project_name, dirac_user=self.dirac_user
        ):
            return
        # Note: The two commands ``gb2_job_delete`` and ``gb2_job_kill`` differ
        # in that deleted jobs are killed and removed from the job database,
        # while only killed jobs can be restarted.
        command = shlex.split(
            f"gb2_job_kill --force --user {self.dirac_user} -p {self.gbasf2_project_name}"
        )
        run_with_gbasf2(command)

    def _build_gbasf2_submit_command(self):
        """
        Function to create the gbasf2 submit command to pass to run_with_gbasf2
        from the task options and attributes.
        """
        gbasf2_release = get_setting(
            "gbasf2_release", default=get_basf2_git_hash(), task=self.task
        )
        gbasf2_additional_files = get_setting(
            "gbasf2_additional_files", default=[], task=self.task
        )
        if not isinstance(gbasf2_additional_files, Iterable) or isinstance(
            gbasf2_additional_files, str
        ):
            raise ValueError(
                "``gbasf2_additional_files`` is not an iterable or strings."
            )
        gbasf2_input_sandbox_files = [os.path.basename(self.pickle_file_path)] + list(
            gbasf2_additional_files
        )
        gbasf2_command_str = (
            f"gbasf2 {self.wrapper_file_path} -f {' '.join(gbasf2_input_sandbox_files)} "
            + f"-p {self.gbasf2_project_name} -s {gbasf2_release} "
        )

        gbasf2_noscout = get_setting("gbasf2_noscout", default=False, task=self.task)
        if gbasf2_noscout:
            gbasf2_command_str += " --noscout "

        gbasf2_input_dataset = get_setting(
            "gbasf2_input_dataset", default=False, task=self.task
        )
        gbasf2_input_dslist = get_setting(
            "gbasf2_input_dslist", default=False, task=self.task
        )

        if gbasf2_input_dataset is not False and gbasf2_input_dslist is not False:
            raise RuntimeError(
                "Can't use both `gbasf2_input_dataset` and `gbasf2_input_dslist` simultaneously."
            )

        if gbasf2_input_dataset is not False:
            gbasf2_command_str += f" -i {gbasf2_input_dataset} "
        elif gbasf2_input_dslist is not False:
            if not os.path.isfile(gbasf2_input_dslist):
                raise FileNotFoundError(
                    errno.ENOTDIR, os.strerror(errno.ENOTDIR), gbasf2_input_dslist
                )
            gbasf2_command_str += (
                f" --input_dslist {os.path.abspath(gbasf2_input_dslist)} "
            )
        else:
            raise RuntimeError(
                "Must set either `gbasf2_input_dataset` or `gbasf2_input_dslist`."
            )

        gbasf2_n_repition_jobs = get_setting(
            "gbasf2_n_repition_job", default=False, task=self.task
        )
        if gbasf2_n_repition_jobs is not False:
            gbasf2_command_str += f" --repetition {gbasf2_n_repition_jobs} "

        gbasf2_input_datafiles = get_setting(
            "gbasf2_input_datafiles", default=[], task=self.task
        )
        if gbasf2_input_datafiles:
            gbasf2_command_str += (
                f" --input_datafiles {' '.join(gbasf2_input_datafiles)}"
            )

        # now add some additional optional options to the gbasf2 job submission string

        # whether to ask user for confirmation before submitting job
        force_submission = get_setting(
            "gbasf2_force_submission", default=True, task=self.task
        )
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
            if not 0 <= priority <= 10:
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

        # Provide a output_ds parameter if the group is not belle
        group_name = get_setting("gbasf2_proxy_group", default="belle")
        if group_name != "belle":
            output_lpn_dir = get_setting("gbasf2_project_lpn_path")
            gbasf2_command_str += (
                f" --output_ds {output_lpn_dir}/{self.gbasf2_project_name}"
            )
        # optional string of additional parameters to append to gbasf2 command
        gbasf2_additional_params = get_setting(
            "gbasf2_additional_params", default=False, task=self.task
        )
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
        from b2luigi.batch.processes.gbasf2_utils.pickle_utils import \
            write_path_and_state_to_file

        write_path_and_state_to_file(basf2_path, self.pickle_file_path)

    def _create_wrapper_steering_file(self):
        """
        Create a steering file to send to the grid that executes the pickled
        basf2 path from ``self.task.create_path()``.
        """
        # read a jinja2 template for the steerinfile that should execute the pickled path
        template_file_path = os.path.join(
            self._file_dir, "templates/gbasf2_steering_file_wrapper.jinja2"
        )
        with open(template_file_path, "r") as template_file:
            template = Template(template_file.read())
            # replace some variable values in the templates
            steering_file_stream = template.stream(
                pickle_file_path=os.path.basename(self.pickle_file_path),
                max_event=get_setting("max_event", default=0, task=self.task),
            )
            # write the template with the replacements to a new file which should be sent to the grid
            steering_file_stream.dump(self.wrapper_file_path)

    def _get_gbasf2_dataset_query(self, output_file_name: str) -> str:
        """
        Helper method that returns the gbasf2 query string with the correct wildcard pattern
        to get the subset of all files for ``output_file_name`` from the grid project associated with this task,
        either, e.g. via the ``gb2_ds_list`` or ``gb2_ds_get`` commands.

        Args:
            output_file_name: Output file name, must be a root file, e.g. ``ntuple.root``.
                Usually defined by the user via :py:func:`b2luigi.Task.add_to_output` in
                the :py:func:`b2luigi.Task.output` method.
        """
        if output_file_name != os.path.basename(output_file_name):
            raise ValueError(
                f'For grid projects, the output file name must be a basename, not a path, but is "{output_file_name}"'
            )
        # split file basename into stem and all extensions (e.g. "file.udst.root" into "file" and ".udst.root")
        output_file_stem, output_file_extensions = _split_all_extensions(
            output_file_name
        )
        if not output_file_extensions.endswith(".root"):
            raise ValueError(
                f'Output file name extensions "{output_file_extensions}" do not '
                'end with ".root", but gbasf2 batch only supports root outputs.'
            )
        output_lpn_dir = f"/belle/user/{self.dirac_user}"
        group_name = get_setting("gbasf2_proxy_group", default="belle")
        if group_name != "belle":
            output_lpn_dir = get_setting("gbasf2_project_lpn_path")
        return f"{output_lpn_dir}/{self.gbasf2_project_name}/sub*/{output_file_stem}_*{output_file_extensions}"

    def _local_gb2_dataset_is_complete(
        self, output_file_name: str, check_temp_dir: bool = False
    ) -> bool:
        """
        Helper method that returns ``True`` if the download of the gbasf2
        dataset for the output ``output_file_name`` is complete.

        Args:
            output_file_name: Output file name, must be a root file, e.g. ``ntuple.root``.
                Usually defined by the user via :py:func:`b2luigi.Task.add_to_output` in
                the :py:func:`b2luigi.Task.output` method.
            check_temp_dir: Instead of checking the final output path, check whether the download into the
                temporary ("partial") directory is complete. This function is usually called with this
                argument set to ``True``, to check whether the dataset can be moved to its final output path.
        """
        # first get the local set of files in the dataset for `output_file_name`
        task_output_dict = flatten_to_dict(self.task.output())
        output_target = task_output_dict[output_file_name]
        output_dir_path = output_target.path
        if check_temp_dir:
            # files in the temporary download directory
            glob_expression = os.path.join(
                f"{output_dir_path}.partial", self.gbasf2_project_name, "sub*", "*.root"
            )
            downloaded_dataset_basenames = [
                os.path.basename(fpath) for fpath in glob(glob_expression)
            ]
        else:
            # file in the final output directory
            downloaded_dataset_basenames = os.listdir(output_dir_path)
        if not downloaded_dataset_basenames:
            return False

        # get the remote set of grid file names for the gbasf2 project output matching output_file_name
        ds_query_string = self._get_gbasf2_dataset_query(output_file_name)
        output_dataset_grid_filepaths = query_lpns(ds_query_string)
        output_dataset_basenames = [
            os.path.basename(grid_path) for grid_path in output_dataset_grid_filepaths
        ]
        # remove duplicate LFNs that gb2_ds_list returns for outputs from rescheduled jobs
        output_dataset_basenames = get_unique_lfns(output_dataset_basenames)
        # check if local and remote datasets are equal
        if set(output_dataset_basenames) == set(downloaded_dataset_basenames):
            return True
        missing_files = list(
            set(output_dataset_basenames).difference(downloaded_dataset_basenames)
        )
        superfluous_files = list(
            set(downloaded_dataset_basenames).difference(output_dataset_basenames)
        )
        if missing_files:
            warnings.warn(
                "\nFiles missing in download:\n{}".format("\n".join(missing_files)),
                RuntimeWarning,
            )
        if superfluous_files:
            warnings.warn(
                "\nFiles superfluous in download:\n{}".format(
                    "\n".join(superfluous_files)
                ),
                RuntimeWarning,
            )
        return False

    def _download_dataset(self):
        """
        Download the task outputs from the gbasf2 project dataset.

        For each task output defined via ``self.add_to_output(<name>.root)`` a
        directory will be created, into which all files named ``name_*.root`` on
        the grid dataset corresponding to the project name will be downloaded.
        The download is ensured to be automatic by first downloading into
        temporary directories.
        """
        if not check_dataset_exists_on_grid(
            self.gbasf2_project_name, dirac_user=self.dirac_user
        ):
            raise RuntimeError(
                f"Not dataset to download under project name {self.gbasf2_project_name}"
            )
        task_output_dict = flatten_to_dict(self.task.output())
        for output_file_name, output_target in task_output_dict.items():
            dataset_query_string = self._get_gbasf2_dataset_query(output_file_name)
            output_dir_path = output_target.path
            # check if dataset had been already downloaded and if so, skip downloading
            if os.path.isdir(output_dir_path) and self._local_gb2_dataset_is_complete(
                output_file_name
            ):
                print(
                    f"Dataset already exists in {output_dir_path}, skipping download."
                )
                continue

            # To prevent from task being accidentally marked as complete when the gbasf2 dataset download failed,
            # we create a temporary directory and first download the dataset there.
            # If the download had been successful and the local files are identical to the list of files on the grid,
            # we move the downloaded dataset to the location specified by ``output_dir_path``.
            tmp_output_dir_path = f"{output_dir_path}.partial"
            tmp_project_dir = os.path.join(
                tmp_output_dir_path, self.gbasf2_project_name
            )
            # if custom group is given we need to append the project name
            group_name = get_setting("gbasf2_proxy_group", default="belle")
            if group_name != "belle":
                tmp_output_dir_path += f"/{self.gbasf2_project_name}"
                tmp_project_dir = tmp_output_dir_path
            os.makedirs(tmp_output_dir_path, exist_ok=True)

            # Need a set files to repeat download for FAILED ones only
            monitoring_failed_downloads_file = os.path.abspath(
                os.path.join(tmp_output_dir_path, "failed_files.txt")
            )
            (
                monitoring_download_file_stem,
                monitoring_downloads_file_ext,
            ) = os.path.splitext(monitoring_failed_downloads_file)
            old_monitoring_failed_downloads_file = (
                f"{monitoring_download_file_stem}_old{monitoring_downloads_file_ext}"
            )

            # In case of first download, the file 'monitoring_failed_downloads_file' does not exist
            if not os.path.isfile(monitoring_failed_downloads_file):
                ds_get_command = shlex.split(
                    f"gb2_ds_get --force {dataset_query_string} "
                    f"--failed_lfns {monitoring_failed_downloads_file}"
                )
                print(
                    f"Downloading dataset into\n  {tmp_output_dir_path}\n  with command\n  ",
                    " ".join(ds_get_command),
                )

            # Any further time is based on the list of files from failed downloads
            else:
                # Rename current 'monitoring_failed_downloads_file' to reuse the path in case the download fails again
                os.rename(
                    monitoring_failed_downloads_file,
                    old_monitoring_failed_downloads_file,
                )
                ds_get_command = shlex.split(
                    f"gb2_ds_get --force {dataset_query_string} "
                    f"--input_dslist {old_monitoring_failed_downloads_file} "
                    f"--failed_lfns {monitoring_failed_downloads_file}"
                )
                print(
                    f"Downloading remaining files into\n  {tmp_output_dir_path}\n  with command\n  ",
                    " ".join(ds_get_command),
                )

            stdout = run_with_gbasf2(
                ds_get_command, cwd=tmp_output_dir_path, capture_output=True
            ).stdout
            print(stdout)
            if "No file found" in stdout:
                raise RuntimeError(
                    f"No output data for gbasf2 project {self.gbasf2_project_name} found."
                )

            # Check, whether a file with failed lfns was created
            failed_files = []
            if os.path.isfile(monitoring_failed_downloads_file):
                with open(
                    monitoring_failed_downloads_file, "r"
                ) as failed_downloads_fileobj:
                    failed_files = [
                        f.strip() for f in failed_downloads_fileobj.readlines()
                    ]

            if not failed_files:
                # In case all downloads were successful:
                # remove 'old_monitoring_failed_downloads_file' and 'monitoring_failed_downloads_file'
                if os.path.isfile(old_monitoring_failed_downloads_file):
                    os.remove(old_monitoring_failed_downloads_file)
                if os.path.isfile(monitoring_failed_downloads_file):
                    os.remove(monitoring_failed_downloads_file)

            if not self._local_gb2_dataset_is_complete(
                output_file_name, check_temp_dir=True
            ):
                raise RuntimeError(
                    f"Download incomplete. The downloaded set of files in {tmp_project_dir} is not equal to the "
                    + f"list of dataset files on the grid for project {self.gbasf2_project_name}.",
                )

            print(
                f"Download of {self.gbasf2_project_name} files successful.\n"
                f"Moving output files to directory: {output_dir_path}"
            )
            # sub00 and other sub<xy> directories in case of other datasets
            _move_downloaded_dataset_to_output_dir(
                project_download_path=tmp_project_dir, output_path=output_dir_path
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
            download_logs_command = shlex.split(
                f"gb2_job_output --user {self.dirac_user} -p {self.gbasf2_project_name}"
            )
            run_with_gbasf2(download_logs_command, cwd=tmpdir_path)
            tmp_gbasf2_log_path = os.path.join(
                tmpdir_path, "log", self.gbasf2_project_name
            )
            gbasf2_project_log_dir = os.path.join(
                self.log_file_dir, "gbasf2_logs", self.gbasf2_project_name
            )
            print(
                f"Download of logs for gbasf2 project {self.gbasf2_project_name} successful.\n"
                f" Moving logs to {gbasf2_project_log_dir}"
            )
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
        if not check_dataset_exists_on_grid(
            self.project_name, dirac_user=self.dirac_user
        ):
            # there's no dataset associated with that name on the grid
            return False
        if check_project_exists(self.project_name, dirac_user=self.dirac_user):
            # if there's data named after that project on the grid, ensure there are no jobs writig to it
            project_status_dict = get_gbasf2_project_job_status_dict(
                self.project_name, self.dirac_user
            )
            all_jobs_done = all(
                job_info["Status"] == "Done" and job_info["ApplicationStatus"] == "Done"
                for job_info in project_status_dict.values()
            )
            if not all_jobs_done:
                return False
        return True


def check_dataset_exists_on_grid(gbasf2_project_name, dirac_user=None):
    """
    Check if an output dataset exists for the gbasf2 project
    """
    output_lpn_dir = gbasf2_project_name
    group_name = get_setting("gbasf2_proxy_group", default="belle")
    if group_name != "belle":
        output_lpn_dir = (
            get_setting("gbasf2_project_lpn_path") + f"/{gbasf2_project_name}"
        )
    lpns = query_lpns(output_lpn_dir, dirac_user=dirac_user)
    return len(lpns) > 0


@retry(
    (json.decoder.JSONDecodeError, subprocess.CalledProcessError),
    tries=4,
    delay=2,
    backoff=3,  # retry after 2,6,18,108 s
)
def get_gbasf2_project_job_status_dict(gbasf2_project_name, dirac_user=None):
    """
    Returns a dictionary for all jobs in the project with a structure like the
    following, which I have taken and adapted from an example output::

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

    For that purpose, the script in ``gbasf2_job_status.py`` is called.  That
    script directly interfaces with Dirac via its API, but it only works with
    the gbasf2 environment and python2, which is why it is called as a
    subprocess.  The job status dictionary is passed to this function via json.
    """
    if dirac_user is None:
        dirac_user = get_dirac_user()
    job_status_script_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)), "gbasf2_utils/gbasf2_job_status.py"
    )
    group_arg = ""
    group_name = get_setting("gbasf2_proxy_group", default="belle")
    if group_name != "belle":
        group_arg = f" --group {group_name}"
    job_status_command = shlex.split(
        f"{job_status_script_path} -p {gbasf2_project_name} --user {dirac_user}{group_arg}"
    )
    proc = run_with_gbasf2(job_status_command, capture_output=True, check=False)
    # FIXME: use enum or similar to define my own return codes
    if proc.returncode == 3:  # return code 3 means project does not exist yet
        raise RuntimeError(
            f"\nCould not find any jobs for project {gbasf2_project_name} on the grid.\n"
            + "Probably there was an error during the project submission when running the gbasf2 command.\n"
        )
    job_status_json_string = proc.stdout
    return json.loads(job_status_json_string)


def check_project_exists(gbasf2_project_name, dirac_user=None):
    """
    Check if we can find the gbasf2 project on the grid with ``gb2_job_status``.
    """
    try:
        return bool(get_gbasf2_project_job_status_dict(gbasf2_project_name, dirac_user))
    except RuntimeError:
        return False


def run_with_gbasf2(
    cmd,
    *args,
    ensure_proxy_initialized=True,
    check=True,
    encoding="utf-8",
    capture_output=False,
    **kwargs,
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
        if kwargs.get("stdout") is not None or kwargs.get("stderr") is not None:
            raise ValueError(
                "stdout and stderr arguments may not be used " "with capture_output."
            )
        kwargs["stdout"] = subprocess.PIPE
        kwargs["stderr"] = subprocess.PIPE
    gbasf2_env = get_gbasf2_env()
    if ensure_proxy_initialized:
        setup_dirac_proxy()
    proc = subprocess.run(
        cmd, *args, check=check, encoding=encoding, env=gbasf2_env, **kwargs
    )
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
        # Use the latest gbasf2 release on CVMFS as the default gbasf2 install directory.
        # To get the directory of the latest release, take the parent of the "pro"
        # symlink which points to a sub-directory of the latest release.
        default_gbasf2_install_directory = os.path.realpath(
            os.path.join("/cvmfs/belle.kek.jp/grid/gbasf2/pro", os.pardir, os.pardir)
        )
        gbasf2_install_directory = get_setting(
            "gbasf2_install_directory", default=default_gbasf2_install_directory
        )
    gbasf2_setup_path = os.path.join(
        gbasf2_install_directory, "BelleDIRAC/gbasf2/tools/setup.sh"
    )
    if not os.path.isfile(gbasf2_setup_path):
        raise FileNotFoundError(
            f"Could not find gbasf2 setup file at:\n{gbasf2_setup_path}.\n"
            f"Make sure that `gbasf2_install_directory` is set correctly. Current setting:\n{gbasf2_install_directory}.\n"
        )
    # complete bash command to set up the gbasf2 environment
    # piping output to /dev/null, because we want that our final script only prints the ``env`` output
    gbasf2_setup_command_str = f"source {gbasf2_setup_path} > /dev/null"
    # command to execute the gbasf2 setup command in a fresh shell and output the produced environment
    echo_gbasf2_env_command = shlex.split(
        f"env -i bash -c '{gbasf2_setup_command_str} > /dev/null && env'"
    )
    gbasf2_env_string = subprocess.run(
        echo_gbasf2_env_command, check=True, stdout=subprocess.PIPE, encoding="utf-8"
    ).stdout
    gbasf2_env = dict(line.split("=", 1) for line in gbasf2_env_string.splitlines())
    # The gbasf2 setup script on sets HOME to /ext/home/ueda if it's unset,
    # which later causes problems in the gb2_proxy_init subprocess. Therefore,
    # reset it to the caller's HOME.
    try:
        gbasf2_env["HOME"] = os.environ["HOME"]
    except KeyError:
        pass
    return gbasf2_env


def get_proxy_info():
    """Run ``gbasf2_proxy_info.py`` to retrieve a dict of the proxy status."""
    proxy_info_script_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)), "gbasf2_utils/gbasf2_proxy_info.py"
    )

    # Setting ``ensure_proxy_initialized=False`` is vital here, otherwise we get
    # an infinite loop because run_with_gbasf2 will then check for the proxy info
    proc = run_with_gbasf2(
        [proxy_info_script_path],
        capture_output=True,
        ensure_proxy_initialized=False,
    )
    return json.loads(proc.stdout)


@lru_cache(maxsize=None)
def get_dirac_user():
    """Get dirac user name."""
    # ensure proxy is initialized, because get_proxy_info can't do it, otherwise
    # it causes an infinite loop
    setup_dirac_proxy()
    try:
        return get_proxy_info()["username"]
    except KeyError as err:
        raise RuntimeError(
            "Could not obtain dirac user name from `gb2_proxy_init` output."
        ) from err


def setup_dirac_proxy():
    """Run ``gb2_proxy_init -g belle`` if there's no active dirac proxy. If there is, do nothing."""
    # first run script to check if proxy is already alive or needs to be initalized
    try:
        if get_proxy_info()["secondsLeft"] > 3600 * get_setting(
            "gbasf2_min_proxy_lifetime", default=0
        ):
            return
    #  error is raised if proxy hasn't been initialized yet, in that case also process with initialization
    except subprocess.CalledProcessError:
        pass

    # initialize proxy
    lifetime = get_setting("gbasf2_proxy_lifetime", default=24)
    group_name = get_setting("gbasf2_proxy_group", default="belle")
    if not isinstance(lifetime, int) or lifetime <= 0:
        warnings.warn(
            "Setting 'gbasf2_proxy_lifetime' should be a positive integer.",
            RuntimeWarning,
        )
    hours = int(lifetime)
    proxy_init_cmd = shlex.split(
        f"gb2_proxy_init -g {group_name} -v {hours}:00 --pwstdin"
    )

    while True:
        pwd = getpass("Certificate password: ")
        try:
            proc = run_with_gbasf2(
                proxy_init_cmd,
                input=pwd,
                ensure_proxy_initialized=False,
                capture_output=True,
                check=True,
            )
        finally:
            del pwd
        # Check if there were any errors, since gb2_proxy_init often still exits without errorcode and sends messages to stdout
        out, err = proc.stdout, proc.stderr
        all_output = (
            out + err
        )  # gb2_proxy_init errors are usually in stdout, but for future-proofing also check stderr

        # if wrong password, retry password entry
        # usually the output then contains "Bad passphrase", but for future-proofing we check "bad pass"
        if "bad pass" in all_output.lower():
            print("Wrong certificate password, please try again.", file=sys.stderr)
            continue

        # for all other errors, raise an exception and abort
        # Usually for errors, the output contains the line: "Error: Operation not permitted ( 1 : )"
        if "error" in all_output.lower():
            raise subprocess.CalledProcessError(
                returncode=errno.EPERM,
                cmd=proxy_init_cmd,
                output=(
                    "There seems to be an error in the output of gb2_proxy_init."
                    f"\nCommand output:\n{out}"
                ),
            )
        # if no  wrong password and no other errors, stop loop and return
        return


def query_lpns(ds_query: str, dirac_user: Optional[str] = None) -> List[str]:
    """
    Query DIRAC for LPNs matching query, and return them as a list.

    This function exists to avoid manual string parsing of ``gb2_ds_list``.
    """
    if dirac_user is None:
        dirac_user = get_dirac_user()

    ds_list_script_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)), "gbasf2_utils/gbasf2_ds_list.py"
    )

    query_cmd = [ds_list_script_path, "--dataset", ds_query, "--user", dirac_user]
    proc = run_with_gbasf2(
        query_cmd,
        capture_output=True,
        ensure_proxy_initialized=True,
    )

    lpns = json.loads(proc.stdout)
    if not isinstance(lpns, list):
        raise TypeError(
            f"Expected gbasf2 dataset query to return list, but instead the command\n{query_cmd}\n"
            + f"returned: {lpns}"
        )
    return lpns


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
        gbasf2_project_name_prefix = get_setting(
            "gbasf2_project_name_prefix", task=task
        )
    except AttributeError as err:
        raise Exception(
            "Task can only be used with the gbasf2 batch process if it has ``gbasf2_project_name_prefix`` "
            + "as a luigi parameter, attribute or setting."
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
            f'Project name "{gbasf2_project_name}" is invalid. '
            "Only alphanumeric project names are officially supported by gbasf2."
        )
    return gbasf2_project_name


def lfn_follows_gb2v5_convention(lfn: str) -> bool:
    """
    Check if the LFN follows the convention of gbasf2 release 5, i.e.
       <name>_<gbasf2param>_<jobID>_<rescheduleNum>.root

    Args:
        lfn: Logical file name, a file path on the grid
    """
    # adapted the logic from the BelleDirac function ``findDuplicatedJobID()``
    if len(lfn.split("_")) < 2 or "job" not in lfn.split("_")[-2]:
        return False
    return True


def _get_lfn_upto_reschedule_number(lfn: str) -> str:
    """
    Get a string of the gbasf2 v5 LFN upto the reschule number.

    E.g. if the LFN is ``<name>_<gbasf2param>_<jobID>_<rescheduleNum>.root``
    return ````<name>_<gbasf2param>_<jobID>``.
    """
    if not lfn_follows_gb2v5_convention(lfn):
        raise ValueError(
            "LFN does does not conform to the new gbasf2 v5 style, "
            "thus getting the substring upto the reschedule number does not make sense"
        )
    return "_".join(lfn.split("_")[:-1])


def get_unique_lfns(lfns: Iterable[str]) -> Set[str]:
    """
    From list of gbasf2 LFNs which include duplicate outputs for rescheduled
    jobs return filtered list which only include the LFNs for the jobs with the
    highest reschedule number.

    Gbasf2 v5 has outputs of the form
    ``<name>_<gbasf2param>_<jobID>_<rescheduleNum>.root``.  When using
    ``gb2_ds_list``, we see duplicates LFNs where all parts are the same accept
    the ``rescheduleNum``.  This function returns only those with the highest number.
    """
    # if dataset does not follow gbasf2 v5 convention, assume it was produced
    # with old release and does not contain duplicates
    if not all(lfn_follows_gb2v5_convention(lfn) for lfn in lfns):
        return set(lfns)
    # if it is of the gbasf v5 form, group the outputs by the substring upto the
    # reschedule number and return list of maximums of each group
    lfns = sorted(lfns, key=_get_lfn_upto_reschedule_number)
    return {
        max(lfn_group)
        for _, lfn_group in groupby(lfns, key=_get_lfn_upto_reschedule_number)
    }


def _move_downloaded_dataset_to_output_dir(
    project_download_path: str, output_path: str
) -> None:
    """
    Move files downloaded downloaded to the grid to their final output location.

    In the ``Gbasf2Process``, the outputs are usually downloaded with ``gb2_ds_get`` to
    a temporary directory (``project_download_path``)  with a structure like

        <result_dir>/B.root.partial/<project_name>
        ├── sub00/job_name*B.root
        ├── sub01/job_name*B.root
        ├── …

    This function moves those files to their final ``output_path`` directory which has
    the same name as the original root file (e.g. ``B.root``) to fullfill the luigi
    output definition. This output directory has the structure

        <result_dir>/B.root/job_name*B.root

    :param project_download_path: Directory into which ``gb2_ds_get`` downloaded the
        grid dataset. The contents should be ``sub<xy>`` datablocks containing root files.
    :param output_path: Final output directory into which the ROOT files should be copied.
    """
    # the download shouldn't happen if the output already exists, but assert that's the case just to be sure
    if os.path.exists(output_path):
        raise FileExistsError(f"Output directory {output_path} already exists.")
    sub_directories = glob(os.path.join(project_download_path, "sub*"))
    try:
        os.makedirs(output_path, exist_ok=True)
        for sub_dir in sub_directories:
            root_fpaths = glob(os.path.join(sub_dir, "*.root"), recursive=False)
            if not root_fpaths:
                raise RuntimeError(f"Cannot find any .root files in {sub_dir}.")
            for root_fpath in root_fpaths:
                shutil.move(src=root_fpath, dst=output_path)
    except BaseException:
        # In case the moving fails (should never happen), remove output_path to make sure that the
        # task isn't accidentally marked as complete
        shutil.rmtree(output_path, ignore_errors=True)
        raise


def _split_all_extensions(path: str) -> Tuple[str, str]:
    """
    Split all extensions from a string pathname.

    Similar to ``os.path.splitext``, but with the difference that the
    extensions-part is considered everything from the *first* non-leading dot to
    to the end. Leading dots at the beginning of ``path`` are considered part of
    the stem and not considered an extensions separator.

    Returns ``(stem, extensions)``.

    ``extensions`` may be empty if ``path`` does not contain non-leading dots.
    """
    dirname, basename = os.path.split(path)
    basename_no_leading_dots = basename.lstrip(".")
    num_leading_dots = len(basename) - len(basename_no_leading_dots)
    leading_dots = "." * num_leading_dots

    ext_split_output = basename_no_leading_dots.split(".", maxsplit=1)
    # path didn't contain non-leading dots, doesn't contain extensions
    if len(ext_split_output) == 1:
        base_stem_no_leading_dots = ext_split_output[0]
        extensions = ""
    elif len(ext_split_output) == 2:
        base_stem_no_leading_dots, extensions_no_leading_dot = ext_split_output
        extensions = "." + extensions_no_leading_dot
    else:
        raise RuntimeError(
            f"split_output {ext_split_output} can only contain 1 or 2 elements."
        )
    base_stem = leading_dots + base_stem_no_leading_dots
    stem = os.path.join(dirname, base_stem)
    return stem, extensions
