import os
import subprocess
import functools
import sys

from b2luigi.core.settings import get_setting
from b2luigi.core.task import Task
from b2luigi.core.utils import create_output_dirs
from b2luigi.core.executable import run_task_remote


def dispatch(run_function):
    """
    In cases you have a run function calling external, probably insecure functionalities,
    use this function wrapper around your run function.
    It basically `emulates` a batch submission on your local computer (without any
    batch system) with the benefit of having a totally separete execution path.
    If your called task fails miserably (e.g. segfaults), it does not crash your main application.

    Example:
        The run function can include any code you want. When the task runs,
        it is started in a subprocess and monitored by the parent process.
        When it dies unexpectedly (e.g. because of a segfault etc.)
        the task will be marked as failed. If not, it is successful.
        The log output will be written to two files in the log folder (marked with 
        the parameters of the task), which you can check afterwards::

            import b2luigi

            class MyTask(b2luigi.Task):
                @b2luigi.dispatch
                def run(self):
                    call_some_evil_function()

    Note:
        We are reusing the batch system implementation here, with all its settings
        and nobs to setup the environment etc.
        If you want to control it in more detail, please check out :ref:`batch-label`.

    Implementation note:
        In the subprocess we are calling the current executable (which should by python) 
        with the current input file as a parameter, but let it only run this
        specific task (by handing over the task id and the `--batch-worker` option).
        The run function notices this and actually runs the task instead of dispatching again.

    Additionally, you can add a ``cmd_prefix`` parameter to your class, which also
    needs to be a list of strings, which are prefixed to the current command (e.g.
    if you want to add a profiler to all your tasks).
    """

    @functools.wraps(run_function)
    def wrapped_run_function(task):
        if get_setting("local_execution", False):
            create_output_dirs(task)
            run_function(task)
        else:
            run_task_remote(task)

    return wrapped_run_function


class DispatchableTask(Task):
    """
    Instead of using the :obj:`dispatch` function wrapper,
    you can also inherit from this class.
    Except that, it has exactly the same functionality
    as a normal :obj:`Task`.

    Important: 
        You need to overload the process function
        instead of the run function in this case!
    """

    def process(self):
        """
        Override this method with your normal run function.
        Do not touch the run function itself!
        """
        raise NotImplementedError

    def run(self):
        dispatch(self.__class__.process)(self)
