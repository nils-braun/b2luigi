import contextlib
import os
import subprocess
import sys
import types

import colorama

from b2luigi.core.settings import get_setting
from b2luigi.core.task import Task
from b2luigi.core.utils import get_log_files


def _on_failure_for_dispatch_task(self, exception):
    stdout_file_name, stderr_file_name = get_log_files(self)

    print(colorama.Fore.RED)
    print("Task", self.task_family, "failed!")
    print("Parameters")
    for key, value in self.get_filled_params().items():
        print("\t",key, "=", value)
    print("Please have a look into the log files")
    print(stdout_file_name)
    print(stderr_file_name)
    print(colorama.Style.RESET_ALL)

def _run_task_locally(task, run_function):
    task.create_output_dirs()
    run_function(task)

def _run_task_remote(task):
    task.on_failure = types.MethodType(_on_failure_for_dispatch_task, task)

    filename = os.path.realpath(sys.argv[0])
    stdout_file_name, stderr_file_name = get_log_files(task)

    cmd = []
    if hasattr(task, "cmd_prefix"):
        cmd = task.cmd_prefix

    executable = get_setting("executable", [sys.executable])
    cmd += executable
    
    cmd += [os.path.basename(filename), "--batch-runner", "--task-id", task.task_id]

    with open(stdout_file_name, "w") as stdout_file:
        with open(stderr_file_name, "w") as stderr_file:
            return_code = subprocess.call(cmd, stdout=stdout_file, stderr=stderr_file,
                                          cwd=os.path.dirname(filename))

    if return_code:
        raise RuntimeError(f"Execution failed with return code {return_code}")



def dispatch(run_function):
    def wrapped_run_function(self):
        if get_setting("local_execution", False):
            _run_task_locally(self, run_function)
        else:
            _run_task_remote(self)

    return wrapped_run_function


class DispatchableTask(Task):
    def process(self):
        raise NotImplementedError

    def run(self):
        dispatch(self.__class__.process)(self)
