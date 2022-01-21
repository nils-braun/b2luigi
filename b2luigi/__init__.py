"""Task scheduling and batch running for basf2 jobs made simple"""
from luigi import *
from luigi.util import copies

# version must be defined after importing the luigi namespace,
# otherwise the b2luigi.__version__ gets overwritten by the one from luigi
__version__ = "0.7.5"

from b2luigi.core.parameter import wrap_parameter, BoolParameter
from typing import Optional, Union, Collection

wrap_parameter()

from b2luigi.core.task import Task, ExternalTask, WrapperTask
from b2luigi.core.temporary_wrapper import on_temporary_files
from b2luigi.core.dispatchable_task import DispatchableTask, dispatch
from b2luigi.core.settings import get_setting, set_setting, clear_setting
from b2luigi.cli.process import process


class requires(object):
    """
    This "hack" copies the luigi.requires functionality, except that we allow for
    additional kwarg arguments when called.

    It can be used to require a certain task, but with some variables already set,
    e.g.

        class TaskA(b2luigi.Task):
             some_parameter = b2luigi.IntParameter()
             some_other_parameter = b2luigi.IntParameter()

             def output(self):
                 yield self.add_to_output("test.txt")

         @b2luigi.requires(TaskA, some_parameter=3)
         class TaskB(b2luigi.Task):
             another_parameter = b2luigi.IntParameter()

             def output(self):
                 yield self.add_to_output("out.dat")

    TaskB will not require TaskA, where some_parameter is already set to 3.
    This also means, that TaskB only has the parameters another_parameter
    and some_other_parameter (because some_parameter is already fixed).

    """

    def __init__(self, task_to_require, **kwargs):
        super(requires, self).__init__()
        self.kwargs = kwargs
        self.task_to_require = task_to_require

    def __call__(self, task_that_requires):
        # Get all parameter objects from the underlying task
        for param_name, param_obj in self.task_to_require.get_params():
            # Check if the parameter exists in the inheriting task
            if not hasattr(task_that_requires, param_name) and param_name not in self.kwargs:
                # If not, add it to the inheriting task
                setattr(task_that_requires, param_name, param_obj)

        old_requires = task_that_requires.requires

        # Modify task_that_requres by adding methods
        def requires(_self):
            yield from old_requires(_self)
            yield _self.clone(cls=self.task_to_require, **self.kwargs)

        task_that_requires.requires = requires

        return task_that_requires


class inherits(object):

    """
    This copies the luigi.inherits functionality but allows specifying parameters you
    don't want to inherit.

    It can e.g. be used in tasks that merge the output of the tasks they require. These merger tasks don't need
    the parameter they resolve anymore but should keep the same order of parameters, therefore simplifying the directory
     structure created by `b2luigi.Task.add_to_output`.

    Usage can be similar to this:

        class TaskA(b2luigi.Task):
             some_parameter = b2luigi.IntParameter()
             some_other_parameter = b2luigi.IntParameter()

             def output(self):
                 yield self.add_to_output("test.txt")

         @b2luigi.inherits(TaskA, without='some_other_parameter')
         class TaskB(b2luigi.Task):
             another_parameter = b2luigi.IntParameter()

             def requires(self):
                for my_other_parameter in range(10):
                    yield self.clone(TaskA, some_other_parameter=my_other_parameter)

            def run(self):
                # somehow merge the output of TaskA to create "out.dat"
                pass

             def output(self):
                 yield self.add_to_output("out.dat")

    Parameters:
        without: Either a string or a collection of strings

    See also: `b2luigi.requires` which extends ``luigi.requires``.

    """

    def __init__(self, *tasks_to_inherit, without: Optional[Union[Collection[str], str]] = None):
        super(inherits, self).__init__()
        if not tasks_to_inherit:
            raise TypeError("tasks_to_inherit cannot be empty")

        self.tasks_to_inherit = tasks_to_inherit
        if isinstance(without, str):
            self.without = [without]
        elif without is None:
            self.without = []
        else:
            self.without = list(without)

    def __call__(self, task_that_inherits):
        # Get all parameter objects from each of the underlying tasks

        for task_to_inherit in self.tasks_to_inherit:
            for param_name, param_obj in task_to_inherit.get_params():
                # Check if the parameter exists in the inheriting task and isn't in without
                if not hasattr(task_that_inherits, param_name) and param_name not in self.without:
                    # If not, add it to the inheriting task
                    setattr(task_that_inherits, param_name, param_obj)
                elif param_name in self.without:
                    self.without.remove(param_name)

        # Make sure we're only removing parameters that exist
        assert not self.without, f"You're trying to remove parameter(s) {self.without} which do(es) not exist."

        # Modify task_that_inherits by adding methods
        def clone_parent(_self, **kwargs):
            return _self.clone(cls=self.tasks_to_inherit[0], **kwargs)
        task_that_inherits.clone_parent = clone_parent

        def clone_parents(_self, **kwargs):
            return [
                _self.clone(cls=task_to_inherit, **kwargs)
                for task_to_inherit in self.tasks_to_inherit
            ]
        task_that_inherits.clone_parents = clone_parents

        return task_that_inherits
