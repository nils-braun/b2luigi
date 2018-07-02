from . import core

import luigi
import basf2
import ROOT

import inspect
import subprocess


class Basf2Task(core.DispatchableTask):
    num_processes = luigi.IntParameter(significant=False, default=0)
    max_event = luigi.IntParameter(default=0)

    def create_path(self):
        raise NotImplementedError()

    def process(self):
        if self.num_processes:
            basf2.set_nprocesses(self.num_processes)

        if self.max_event:
            ROOT.Belle2.Environment.Instance().setNumberEventsOverride(self.max_event)

        path = self.create_path()

        basf2.print_path(path)
        basf2.process(path)

        print(basf2.statistics)


class Basf2PathCreatorTask(Basf2Task):
    path_creator_function = core.FunctionParameter()

    def create_path(self):
        path_creator_function = self.path_creator_function
        parameters = inspect.signature(path_creator_function).parameters

        path_function_parameters = {}
        for key, value in self.get_filled_params().items():
            if key in parameters:
                path_function_parameters[key] = value

        return path_creator_function(**path_function_parameters)

    def complete(self):
        return False

class Basf2PathTask(Basf2Task):
    basf2_path = None
    
    def create_path(self):
        return self.basf2_path


class HaddTask(core.Task):
    def output(self):
        output_targets = {}

        for key, _ in self.get_transposed_input_file_names():
            if hasattr(self, "keys") and key not in self.keys:
                continue

            output_targets.update(self.get_output_dict([key]))

        return output_targets

    def run(self):
        self.create_output_dirs()

        for key, file_list in self.get_transposed_input_file_names():
            if hasattr(self, "keys") and key not in self.keys:
                continue

            args = ["hadd", "-f", self.get_output_file_name(key)] + file_list
            subprocess.check_call(args)


class Basf2FileMergeTask(HaddTask):
    def run(self):
        self.create_output_dirs()

        for key, file_list in self.get_transposed_input_file_names():
            if hasattr(self, "keys") and key not in self.keys:
                continue
            args = ["b2file-merge", "-f", self.get_output_file_name(key)] + file_list
            subprocess.check_call(args)