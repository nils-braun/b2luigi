import contextlib
import os
import subprocess
import sys

import colorama

from b2luigi.core.settings import get_setting
from b2luigi.core.task import Task


class DispatchableTask(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        filename = os.path.realpath(sys.argv[0])
        log_folder = get_setting("log_folder", default=os.path.join(os.path.dirname(filename), "logs"))
        stdout_file_name = self._get_output_file_name(self.get_task_family() + "_stdout", create_folder=True,
                                                     result_path=log_folder)

        stderr_file_name = self._get_output_file_name(self.get_task_family() + "_stderr", create_folder=True,
                                                     result_path=log_folder)

        self.log_files = {"stdout":  stdout_file_name,
                          "stderr": stderr_file_name,
                          "log_folder": log_folder}


    def get_log_output_files(self):
        """Return the log files for convenience"""
        return self.log_files

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
        stdout_file_name = self.log_files["stdout"]
        stderr_file_name = self.log_files["stderr"]

        with contextlib.suppress(FileNotFoundError):
            os.remove(stdout_file_name)
        with contextlib.suppress(FileNotFoundError):
            os.remove(stderr_file_name)

        process_env = os.environ.copy()

        with open(stdout_file_name, "w") as stdout_file:
            with open(stderr_file_name, "w") as stderr_file:
                return_code = subprocess.call([sys.executable, os.path.basename(filename), "--batch-runner", "--task-id", self.task_id],
                                              stdout=stdout_file, stderr=stderr_file,
                                              env=process_env, cwd=os.path.dirname(filename))

        if return_code:
            raise RuntimeError(f"Execution failed with return code {return_code}")

    def on_failure(self, exception):
        log_files = self.get_log_output_files()

        print(colorama.Fore.RED)
        print("Task", self.task_family, "failed!")
        print("Parameters")
        for key, value in self.get_filled_params().items():
            print("\t",key, "=", value)
        print("Please have a look into the log files")
        print(log_files["stderr"])
        print(log_files["stdout"])
        print(colorama.Style.RESET_ALL)