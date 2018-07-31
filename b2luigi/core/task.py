from b2luigi.core import utils

import luigi

from b2luigi.core.utils import create_output_file_name


class Task(luigi.Task):
    def get_filled_params(self):
        """Helper function for getting the parameter list with each parameter set to its current value"""
        return {key: getattr(self, key) for key, _ in self.get_params()}

    def add_to_output(self, output_file_name):
        """Call this in your output() function"""
        return {output_file_name: self._get_output_file_target(output_file_name, create_folder=True)}

    def get_input_file_names(self, key=None):
        """Get the dict of input file names"""
        input_list = utils.flatten_to_list_of_dicts(self.input())
        file_paths = utils.flatten_to_file_paths(input_list)

        if key is not None:
            return file_paths[key]
        else:
            return file_paths

    def get_output_file_names(self):
        """Get the dict of output file names"""
        output_dict = utils.flatten_to_dict(self.output())
        return utils.flatten_to_file_paths(output_dict)

    def get_output_file(self, key):
        """Shortcut to get the output target for a given key"""
        output_dict = utils.flatten_to_dict(self.output())
        return output_dict[key]

    def create_output_dirs(self):
        """Create all needed output dicts if needed"""
        output_list = utils.flatten_to_dict(self.output())
        output_list = output_list.values()

        for output in output_list:
            output.makedirs()

    def _get_output_file_target(self, base_filename, **kwargs):
        file_name = create_output_file_name(self, base_filename, **kwargs)
        return luigi.LocalTarget(file_name)


class ExternalTask(Task, luigi.ExternalTask):
    pass


class WrapperTask(Task, luigi.WrapperTask):
    pass


class NotCompletedTask(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.check_complete = True

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
