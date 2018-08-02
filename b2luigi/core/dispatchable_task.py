import contextlib
import os
import subprocess
import sys

import colorama

from b2luigi.core.settings import get_setting
from b2luigi.core.task import Task
from b2luigi.core.utils import get_log_files

def dispatch(run_function):
    def wrapped_run_function(self):
        if get_setting("local_execution", False):
            self.create_output_dirs()
            run_function(self)
        else:
            filename = os.path.realpath(sys.argv[0])
            stdout_file_name, stderr_file_name = get_log_files(self)

            executable = get_setting("executable", [sys.executable])

            with open(stdout_file_name, "w") as stdout_file:
                with open(stderr_file_name, "w") as stderr_file:
                    return_code = subprocess.call(self.cmd_prefix + executable + [os.path.basename(filename), "--batch-runner", "--task-id", self.task_id],
                                                stdout=stdout_file, stderr=stderr_file,
                                                cwd=os.path.dirname(filename))

            if return_code:
                raise RuntimeError(f"Execution failed with return code {return_code}")

    return wrapped_run_function


class DispatchableTask(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cmd_prefix = []

    def process(self):
        raise NotImplementedError

    def run(self):
        dispatch(self.__class__.process)(self)

    def on_failure(self, exception):
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
