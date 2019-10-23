import os
import sys
import time
import abc
import enum

import luigi
import luigi.scheduler

from b2luigi.core.utils import on_failure, create_cmd_from_task


class JobStatus(enum.Enum):
    running = "running"
    successful = "successful"
    aborted = "aborted"
    idle = "idle"


class BatchProcess:
    """
    This is the base class for all batch algorithms that allow luigi to run on a specific batch system.
    This is an abstract base class and inheriting classes need to supply functionalities for
    * starting a job using the commands in self.task_cmd
    * getting the job status of a running, finished or failed job
    * and killing a job
    All those commands are called from the main process, which is not running on the batch system.
    Every batch system that is capable of these functions can in principle work together with b2luigi.

    Implementation note:
        In principle, using the batch system is transparent to the user. In case of problems, it
        may however be useful to understand how it is working.

        When you start your luigi dependency tree with ``process(..., batch=True)``, the normal
        luigi process is started looking for unfinished tasks and running them etc.
        Normally, luigi creates a process for each running task and runs them either directly
        or on a different core (if you have enabled more than one worker).
        In the batch case, this process is not a normal python multiprocessing process,
        but this BatchProcess, which has the same interface (one can check the status of the process,
        start or kill it). The process does not need to wait for the batch job to finish but
        is asked repeatedly for the job status. By this, most of the core functionality of luigi
        is kept and reused.
        This also means, that every batch job only includes a single task and is finished whenever
        this task is done decreasing the batch runtime. You will need exactly as many batch jobs
        as you have tasks and no batch job will idle waiting for input data as all are scheduled
        only when the task they should run is actually runnable (the input files are there).

        What is the batch command now? In each job, we call a specific executable bash script
        only created for this task. It contains the setup of the environment (if given by the
        user via the settings), the change of the working directory (the directory of the
        python script or a specified directory by the user) and a call of this script with the 
        current python interpreter (the one you used to call this main file or given by the 
        setting ``executable``) . However, we give this call an additional parameter, which tells it 
        to only run one single task. Task can be identified by their task id. A typical task command may look like::

            /<path-to-your-exec>/python /your-project/some-file.py --batch-runner --task-id MyTask_38dsf879w3

        if the batch job should run the MyTask. The implementation of the
        abstract functions is responsible for creating an running the executable file and writing the log of
        the job into appropriate locations. You can use the functions ``create_executable_wrapper``
        and ``get_log_file_dir`` to get the needed information.

        Checkout the implementation of the lsf task for some implementation example.
    """
    def __init__(self, task, scheduler, result_queue, worker_timeout):
        self.use_multiprocessing = False
        self.task = task
        self.timeout_time = time.time() + worker_timeout if worker_timeout else None
        self._terminated = False

        self._result_queue = result_queue
        self._scheduler = scheduler

    @property
    def exitcode(self):
        # We cheat here a bit: if the exit code is set to 0 all the time, we can always use the result queue for
        # delivering the result
        return 0

    def get_job_status(self):
        """
        Implement this function to return the current job status.
        How you identify exactly your job is dependent on the implementation and needs to
        be handled by your own child class.

        Must return one item of the JobStatus enumeration: running, aborted, successful or idle.
        Will only be called after the job is started but may also be called when
        the job is finished already.
        If the task status is unknown, return aborted. If the task has not started already but
        is scheduled, return running nevertheless (for b2luigi it makes no difference).
        No matter if aborted via a call to kill_job, by the batch system or by an exception in the
        job itself, you should return aborted if the job is not finished successfully 
        (maybe you need to check the exit code of your job).
        """
        raise NotImplementedError

    def start_job(self):
        """
        Override this function in your child class to start a job on the batch system.
        It is called exactly once. You need to store any information identifying
        your batch job on your own.

        You can use the ``b2luigi.core.utils.get_log_file_dir`` and the 
        ``b2luigi.core.executable.create_executable_wrapper`` functions to get the log base name
        and to create the executable script which you should call in your batch job. 

        After the start_job function is called by the framework (and no exception is thrown),
        it is assumed that a batch job is started or scheduled. 

        After the job is finished (no matter if aborted or successful) we assume the stdout and stderr
        is written into the two files given by b2luigi.core.utils.get_log_file_dir(self.task).
        """
        raise NotImplementedError

    def kill_job(self):
        """
        This command is used to abort a job started by the start_job function.
        It is only called once to abort a job, so make sure to either block until the job is really
        gone or be sure that it will go down soon. Especially, do not wait until the job is finished.
        It is called for example when the user presses Ctrl-C.

        In some strange corner cases it may happen that this function is called even before the
        job is started (the start_job function is called). In this case, you do not need to do anything
        (but also not raise an exception).
        """
        raise NotImplementedError

    def run(self):
        self.start_job()

    def terminate(self):
        self.kill_job()

    def is_alive(self):
        if self._terminated:
            return False

        job_status = self.get_job_status()

        if job_status == JobStatus.successful:
            job_output = ""
            self._put_to_result_queue(status=luigi.scheduler.DONE, explanation=job_output)
            self._terminated = True
            return False
        elif job_status == JobStatus.aborted:
            job_output = ""
            self._put_to_result_queue(status=luigi.scheduler.FAILED, explanation=job_output)
            on_failure(self.task, job_output)
            self._terminated = True
            return False
        elif job_status == JobStatus.running:
            return True

        raise ValueError("get_job_status() returned an unknown job state!")

    def _put_to_result_queue(self, status, explanation):
        missing = []
        new_deps = []
        self._result_queue.put((self.task.task_id, status, explanation, missing, new_deps))
