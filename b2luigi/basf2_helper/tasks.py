import collections
import os

import b2luigi
from b2luigi.basf2_helper.targets import ROOTLocalTarget
from b2luigi.basf2_helper.utils import get_basf2_git_hash

import subprocess


class Basf2Task(b2luigi.DispatchableTask):
    git_hash = b2luigi.Parameter(default=get_basf2_git_hash())

    def get_output_file_target(self, *args, **kwargs):
        file_name = self.get_output_file_name(*args, **kwargs)
        if os.path.splitext(file_name)[-1] == ".root":
            return ROOTLocalTarget(file_name)
        else:
            return super().get_output_file_target(*args, **kwargs)

    def get_serialized_parameters(self):
        serialized_parameters = super().get_serialized_parameters()

        # Git hash should go to the front
        return_dict = collections.OrderedDict()
        return_dict["git_hash"] = serialized_parameters["git_hash"]

        for key, value in serialized_parameters.items():
            return_dict[key] = value

        return return_dict


class Basf2PathTask(Basf2Task):
    num_processes = b2luigi.IntParameter(significant=False, default=0)
    max_event = b2luigi.IntParameter(significant=False, default=0)

    def create_path(self):
        raise NotImplementedError()

    def process(self):
        assert get_basf2_git_hash() == self.git_hash

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


class SimplifiedOutputBasf2Task(Basf2PathTask):
    def create_path(self):
        raise NotImplementedError()

    def output(self):
        path = self.create_path()
        outputs = []

        for module in path.modules():
            if module.type() == "RootOutput":
                for param in module.available_params():
                    if param.name == "outputFileName":
                        outputs.append(ROOTLocalTarget(param.values))

        return outputs


class MergerTask(Basf2Task):
    cmd = []

    def output(self):
        for key, _ in self.get_input_file_names().items():
            if hasattr(self, "keys") and key not in self.keys:
                continue

            yield self.add_to_output(key)

    def process(self):
        self.create_output_dirs()

        for key, file_list in self.get_input_file_names().items():
            if hasattr(self, "keys") and key not in self.keys:
                continue

            args = self.cmd + [self.get_output_file_name(key)] + file_list
            subprocess.check_call(args)

class HaddTask(MergerTask):
    cmd = ["hadd", "-f"]

class Basf2FileMergeTask(MergerTask):
    cmd = ["b2file-merge", "-f"]
