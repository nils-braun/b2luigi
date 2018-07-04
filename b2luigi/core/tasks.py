from b2luigi.core import utils
from b2luigi.core.settings import get_setting

import ROOT
import luigi

import os
import sys
import collections
import enum
import contextlib
import subprocess


class NonParseableParameter(luigi.Parameter):
    def parse(self, value):
        raise NotImplementedError()


class FunctionParameter(NonParseableParameter):
    def serialize(self, value):
        return value.__name__


class ROOTLocalTarget(luigi.LocalTarget):
    def exists(self):
        if not super().exists():
            return False

        path = self.path
        tfile = ROOT.TFile.Open(path)
        return tfile and len(tfile.GetListOfKeys()) > 0


class Task(luigi.Task):
    git_hash = luigi.Parameter(default=utils.get_basf2_git_hash())

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        filename = os.path.realpath(sys.argv[0])
        log_folder = os.path.join(os.path.dirname(filename), "logs")
        stdout_file_name = self.get_output_file_name(self.get_task_family() + "_stdout", create_folder=True,
                                                     result_path=log_folder)

        stderr_file_name = self.get_output_file_name(self.get_task_family() + "_stderr", create_folder=True,
                                                     result_path=log_folder)

        self.log_files = {"stdout": stdout_file_name,
                          "stderr": stderr_file_name,
                          "log_folder": log_folder}

        self.check_complete = True

    def get_filled_params(self):
        return {key: getattr(self, key) for key, _ in self.get_params()}

    def get_output_dict(self, output_file_names):
        assert not isinstance(output_file_names, str)
        return {key: self.get_output_file_target(key) for key in output_file_names}

    def get_log_output_files(self):
        return self.log_files

    def get_transposed_input_file_names(self):
        input_file_names = self.get_input_file_names()

        if not input_file_names:
            return

        if not isinstance(input_file_names, list):
            raise TypeError

        return_dict = collections.defaultdict(list)

        for file_names in input_file_names:
            for key, file_name in file_names.items():
                return_dict[key].append(file_name)

        return [(key, value) for key, value in return_dict.items()]

    def get_input_file_names(self):
        return utils.flatten_to_file_paths(self.input())

    def get_output_file_names(self):
        return utils.flatten_to_file_paths(self.output())

    def get_output_file_target(self, *args, **kwargs):
        file_name = self.get_output_file_name(*args, **kwargs)
        if os.path.splitext(file_name)[-1] == ".root":
            return ROOTLocalTarget(file_name)
        else:
            return luigi.LocalTarget(file_name)

    def get_serialized_parameters(self, guess_type=False):
        serialized_parameters = collections.OrderedDict()

        for key, parameter in self.get_params():
            if not parameter.significant:
                continue

            value = getattr(self, key)

            if not guess_type or isinstance(value, enum.Enum):
                value = parameter.serialize(value)

            # TODO: this is a bit unfortunate....
            if isinstance(value, str) and "/" in value:
                value = os.path.splitext(os.path.split(value)[-1])[0]

            serialized_parameters[key] = value

        # Git hash should go to the front
        return_dict = collections.OrderedDict()
        return_dict["git_hash"] = serialized_parameters["git_hash"]

        for key, value in serialized_parameters.items():
            return_dict[key] = value

        return return_dict

    def get_output_file_name(self, base_filename, create_folder=False, result_path=None):
        serialized_parameters = self.get_serialized_parameters()

        if not result_path:
            result_path = get_setting("result_path", ".")

        filename = os.path.join(result_path,
                                *[f"{key}={value}" for key,
                                                       value in serialized_parameters.items()],
                                base_filename)

        if create_folder:
            os.makedirs(os.path.dirname(filename), exist_ok=True)
        return filename

    def complete(self):
        if not super().complete():
            return False

        if not self.check_complete:
            return True

        requires = self.requires()

        try:
            if not requires.complete():
                return False
        except AttributeError:
            for task in requires:
                if not task.complete():
                    return False

        return True

    def create_output_dirs(self):
        output_list = self.output()

        # TODO: this is a hack!
        if isinstance(output_list, dict):
            output_list = output_list.values()

        for output in output_list:
            output.makedirs()


class DispatchableTask(Task):
    cmd_prefix = []

    def process(self):
        raise NotImplementedError

    def run_local(self):
        self.create_output_dirs()

        self.process()

    def run_remote(self):
        env_list = self._prepare_env()
        self.dispatch(os.path.realpath(sys.argv[0]), env_list)

    def run(self):
        if os.environ.get("B2LUIGI_EXECUTION", False) or not get_setting("dispatch", True):
            self.run_local()
        else:
            self.run_remote()

    def dispatch(self, filename, env):
        stdout_file_name = self.log_files["stdout"]
        stderr_file_name = self.log_files["stderr"]

        process_env = os.environ.copy()
        process_env.update(env)

        if get_setting("batch", False):
            return_code = subprocess.call(["bsub", "-K", "-env all", "-eo", stderr_file_name, "-oo", stdout_file_name] +
                                          self.cmd_prefix + [sys.executable, os.path.basename(filename)])
        else:
            with contextlib.suppress(FileNotFoundError):
                os.remove(stdout_file_name)
            with contextlib.suppress(FileNotFoundError):
                os.remove(stderr_file_name)

            with open(stdout_file_name, "w") as stdout_file:
                with open(stderr_file_name, "w") as stderr_file:
                    return_code = subprocess.call(self.cmd_prefix + [sys.executable, os.path.basename(filename)],
                                                  stdout=stdout_file, stderr=stderr_file,
                                                  env=process_env, cwd=os.path.dirname(filename))

        if return_code:
            raise RuntimeError(
                f"Basf2 execution failed with return code {return_code}")

    def _prepare_env(self):
        env = {
            "B2LUIGI_TASK": utils.encode_value(self.__class__),
            "B2LUIGI_EXECUTION": "1"
        }

        for key, parameter in self.get_params():
            value = getattr(self, key)

            env[f"{utils.PREFIX}{key}"] = utils.encode_value(value)
        return env


def run_task_from_env():
    task_class_name = os.environ["B2LUIGI_TASK"]
    task_class = utils.decode_value(task_class_name)

    params = {}

    for key, value in os.environ.items():
        if not key.startswith(utils.PREFIX):
            continue

        params[key.replace(utils.PREFIX, "")] = utils.decode_value(value)

    task = task_class(**params)
    task.run()
