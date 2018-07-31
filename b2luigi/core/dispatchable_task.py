import contextlib
import os
import subprocess
import sys

import colorama

from b2luigi.core.settings import get_setting
from b2luigi.core.task import Task
from b2luigi.core.utils import get_log_files


class DispatchableTask(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cmd_prefix = []

    def process(self):
        raise NotImplementedError

    def run(self):
        if get_setting("local_execution", False):
            self._run_local()
        else:
            self._dispatch()

    def _run_local(self):
        self.create_output_dirs()
        self.process()

    def _dispatch(self):
        filename = os.path.realpath(sys.argv[0])
        stdout_file_name, stderr_file_name = get_log_files(self)

        with open(stdout_file_name, "w") as stdout_file:
            with open(stderr_file_name, "w") as stderr_file:
                return_code = subprocess.call(self.cmd_prefix + [sys.executable, os.path.basename(filename), "--batch-runner", "--task-id", self.task_id],
                                              stdout=stdout_file, stderr=stderr_file,
                                              cwd=os.path.dirname(filename))

        if return_code:
            raise RuntimeError(f"Execution failed with return code {return_code}")

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
