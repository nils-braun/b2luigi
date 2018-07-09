from b2luigi.core import utils
from b2luigi.core.settings import get_setting

import luigi

import os
import collections


class Task(luigi.Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.check_complete = True

    def get_filled_params(self):
        """Helper function for getting the parameter list with each parameter set to its current value"""
        return {key: getattr(self, key) for key, _ in self.get_params()}

    def get_serialized_parameters(self):
        """Get a string-typed ordered dict of key=value for the significant parameters"""
        serialized_parameters = collections.OrderedDict()

        for key, parameter in self.get_params():
            if not parameter.significant:
                continue

            value = getattr(self, key)
            value = parameter.serialize(value)

            serialized_parameters[key] = value

        return serialized_parameters

    def add_to_output(self, output_file_name):
        """Call this in your output() function"""
        return {output_file_name: self._get_output_file_target(output_file_name, create_folder=True)}

    def get_input_file_names(self):
        """Get the dict of input file names"""
        return_dict = collections.defaultdict(list)

        for i in self.input():
            file_names = utils.flatten_to_file_paths(utils.flatten_to_dict(i))

            for key, file_name in file_names.items():
                return_dict[key].append(file_name)

        return {key: value for key, value in return_dict.items()}

    def get_output_file_names(self):
        """Get the dict of output file names"""
        return utils.flatten_to_file_paths(
            utils.flatten_to_dict(self.output())
            )

    def complete(self):
        """Custom complete function checking also the child tasks until a check_complete = False is reached"""
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
        """Create all needed output dicts if needed"""
        output_list = utils.flatten_to_dict(self.output())
        output_list = output_list.values()

        for output in output_list:
            output.makedirs()

    def _get_output_file_target(self, *args, **kwargs):
        file_name = self._get_output_file_name(*args, **kwargs)
        return luigi.LocalTarget(file_name)


    def _get_output_file_name(self, base_filename, create_folder=False, result_path=None):
        serialized_parameters = self.get_serialized_parameters()

        if not result_path:
            result_path = get_setting("result_path", ".")

        param_list = [f"{key}={value}" for key, value in serialized_parameters.items()]
        filename = os.path.join(result_path, *param_list, base_filename)

        if create_folder:
            os.makedirs(os.path.dirname(filename), exist_ok=True)
        return filename


class ExternalTask(Task, luigi.ExternalTask):
    pass

class WrapperTask(Task, luigi.WrapperTask):
    pass