from b2luigi.core import utils

import luigi

from b2luigi.core.utils import create_output_file_name


class Task(luigi.Task):
    """
    Drop in replacement for ``luigi.Task`` which is 100% API compatible.
    It just adds some useful methods for handling output file name generation using
    the parameters of the task.
    See :ref:`quick-start-label` on information on how to use the methods.

    Example:
        .. code-block:: python

          class MyAverageTask(b2luigi.Task):
              def requires(self):
                  for i in range(100):
                      yield self.clone(MyNumberTask, some_parameter=i)

              def output(self):
                  yield self.add_to_output("average.txt")

              def run(self):
                  # Build the mean
                  summed_numbers = 0
                  counter = 0
                  for input_file in self.get_input_file_names("output_file.txt"):
                      with open(input_file, "r") as f:
                          summed_numbers += float(f.read())
                          counter += 1

                  average = summed_numbers / counter

                  with self.get_output_file("average.txt").open("w") as f:
                      f.write(f"{average}\\n")
    """

    def add_to_output(self, output_file_name):
        """
        Call this in your output() function to add a target to the list of files,
        this task will output.
        Always use in combination with `yield`.
        This function will automatically add all current parameter values to
        the file name when used in the form

            result_dir/param_1=value/param_2=value/output_file_name

        This function will automatically use a ``LocalTarget``.
        If you do not want this, you can override the :obj:`_get_output_file_target` function.

        Example:
            This adds two files called ``some_file.txt`` and ``some_other_file.txt`` to the output::

                def output(self):
                    yield self.add_to_output("some_file.txt")
                    yield self.add_to_output("some_other_file.txt")

        Args:
            output_file_name (:obj:`str`): the file name of the output file.
                Refer to this file name as a key when using :obj:`get_input_file_names`,
                :obj:`get_output_file_names` or :obj:`get_output_file`.
        """
        return {output_file_name: self._get_output_file_target(output_file_name)}

    @staticmethod
    def _transform_input(input_generator, key=None):
        input_list = utils.flatten_to_list_of_dicts(input_generator)
        file_paths = utils.flatten_to_file_paths(input_list)

        if key is not None:
            return file_paths[key]
        return file_paths

    def get_all_input_file_names(self):
        """
        Return all file paths required by this task.

        Example:
            class TheSuperFancyTask(b2luigi.Task):
                def dry_run(self):
                    for name in self.get_all_output_file_names():
                        print(f"\t\toutput:\t{name}")
        """
        for file_name_key, file_names in self._transform_input(self.input()).items():
            for file_name in file_names:
                yield file_name

    def get_input_file_names(self, key=None):
        """
        Get a dictionary of input file names of the tasks, which are defined in our requirements.
        Either use the key argument or dictionary indexing with the key given to :obj:`add_to_output`
        to get back a list (!) of file paths.

        Args:
            key (:obj:`str`, optional): If given, only return a list of file paths with this given key.

        Return:
            If key is none, returns a dictionary of keys to list of file paths.
            Else, returns only the list of file paths for this given key.
        """
        return self._transform_input(self.input(), key)

    def get_input_file_names_from_dict(self, requirement_key, key=None):
        """
        Get a dictionary of input file names of the tasks, which are defined in our requirements.

        The requirement method should return a dict whose values are generator expressions (!)
        yielding required task objects.

        Example:
            .. code-block:: python

              class TaskB(luigi.Task):

                  def requires(self):
                      return {
                          "a": (TaskA(5.0, i) for i in range(100)),
                          "b": (TaskA(1.0, i) for i in range(100)),
                      }

                  def run(self):
                      result_a = do_something_with_a(
                          self.get_input_file_names_from_dict("a")
                      )
                      result_b = do_something_with_b(
                          self.get_input_file_names_from_dict("b")
                      )

                      combine_a_and_b(
                          result_a,
                          result_b,
                          self.get_output_file_name("combined_results.txt")
                      )

                  def output(self):
                      yield self.add_to_output("combined_results.txt")


            Either use the key argument or dictionary indexing with the key given to :obj:`add_to_output`
            to get back a list (!) of file paths.

        Args:
            requirement_key (:obj:`str`): Specifies the required task expression.
            key (:obj:`str`, optional): If given, only return a list of file paths with this given key.

        Return:
            If key is none, returns a dictionary of keys to list of file paths.
            Else, returns only the list of file paths for this given key.
        """
        return self._transform_input(self.input()[requirement_key], key)

    @staticmethod
    def _transform_output(output_generator, key=None):
        output_list = utils.flatten_to_list_of_dicts(output_generator)
        file_paths = utils.flatten_to_file_paths(output_list)

        if key is not None:
            return file_paths[key]
        return file_paths

    def get_all_output_file_names(self):
        """
        Return all file paths created by this task.

        Example:
            class TheSuperFancyTask(b2luigi.Task):
                def dry_run(self):
                    for name in self.get_all_output_file_names():
                        print(f"\t\toutput:\t{name}")
        """
        for file_name_key, file_names in self._transform_output(self.output()).items():
            for file_name in file_names:
                yield file_name

    def get_output_file_name(self, key):
        """
        Analogous to :obj:`get_input_file_names` this function returns
        a an output file defined in out output function with
        the given key.

        In contrast to :obj:`get_input_file_names`, only a single file name
        will be returned (as there can only be a single output file with a given name).

        Args:
            key (:obj:`str`): Return the file path with this given key.

        Return:
            Returns only the file path for this given key.
        """
        target = self._get_output_target(key)
        file_paths = utils.flatten_to_file_paths(target)

        return file_paths

    def _get_input_targets(self, key):
        """Shortcut to get the input targets for a given key. Will return a luigi target."""
        input_dict = utils.flatten_to_list_of_dicts(self.input())
        return input_dict[key]

    def _get_output_target(self, key):
        """Shortcut to get the output target for a given key. Will return a luigi target."""
        output_dict = utils.flatten_to_dict(self.output())
        return output_dict[key]

    def _get_output_file_target(self, base_filename, **kwargs):
        file_name = create_output_file_name(self, base_filename, **kwargs)
        return luigi.LocalTarget(file_name)


class ExternalTask(Task, luigi.ExternalTask):
    """Direct copy of :obj:`luigi.ExternalTask`, but with the capabilities of :obj:`Task` added."""
    pass


class WrapperTask(Task, luigi.WrapperTask):
    """Direct copy of :obj:`luigi.WrapperTask`, but with the capabilities of :obj:`Task` added."""
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
