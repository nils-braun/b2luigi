from b2luigi import ROOTLocalTarget
from b2luigi.core import tasks

import luigi

import inspect
import subprocess


class Basf2Task(tasks.DispatchableTask):
    num_processes = luigi.IntParameter(significant=False, default=0)
    max_event = luigi.IntParameter(significant=False, default=0)

    def create_path(self):
        raise NotImplementedError()

    def process(self):
        try:
            import basf2
            import ROOT
        except ImportError:
            raise ImportError("Can not find ROOT or basf2. Can not use the basf2 task.")

        if self.num_processes:
            basf2.set_nprocesses(self.num_processes)

        if self.max_event:
            ROOT.Belle2.Environment.Instance().setNumberEventsOverride(self.max_event)

        path = self.create_path()

        basf2.print_path(path)
        basf2.process(path)

        print(basf2.statistics)


class SimplifiedOutputBasf2Task(Basf2Task):
    def output(self):
        path = self.create_path()
        outputs = []

        for module in path.modules():
            if module.type() == "RootOutput":
                for param in module.available_params():
                    if param.name == "outputFileName":
                        outputs.append(ROOTLocalTarget(param.values))

        return outputs


class Basf2PathCreatorTask(SimplifiedOutputBasf2Task):
    path_creator_function = tasks.FunctionParameter()

    def create_path(self):
        path_creator_function = self.path_creator_function
        parameters = inspect.signature(path_creator_function).parameters

        path_function_parameters = {}
        for key, value in self.get_filled_params().items():
            if key in parameters:
                path_function_parameters[key] = value

        return path_creator_function(**path_function_parameters)


class Basf2PathTask(SimplifiedOutputBasf2Task):
    basf2_path = None
    
    def create_path(self):
        return self.basf2_path


class HaddTask(tasks.Task):
    def output(self):
        output_targets = {}

        for key, _ in self.get_transposed_input_file_names().items():
            if hasattr(self, "keys") and key not in self.keys:
                continue

            yield self.add_to_output(key)

    def run(self):
        self.create_output_dirs()

        for key, file_list in self.get_transposed_input_file_names().items():
            if hasattr(self, "keys") and key not in self.keys:
                continue

            args = ["hadd", "-f", self.get_output_file_name(key)] + file_list
            subprocess.check_call(args)


class Basf2FileMergeTask(HaddTask):
    def run(self):
        self.create_output_dirs()

        for key, file_list in self.get_transposed_input_file_names().items():
            if hasattr(self, "keys") and key not in self.keys:
                continue
            args = ["b2file-merge", "-f", self.get_output_file_name(key)] + file_list
            subprocess.check_call(args)


class Basf2nTupleMergeTask(HaddTask):
    def run(self):
        self.create_output_dirs()

        for key, file_list in self.get_transposed_input_file_names().items():
            if hasattr(self, "keys") and key not in self.keys:
                continue
            args = ["fei_merge_files", self.get_output_file_name(key)] + file_list
            subprocess.check_call(args)
