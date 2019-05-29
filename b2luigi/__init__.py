"""Task scheduling and batch running for basf2 jobs made simple"""
__version__ = "0.3.2"

from luigi import *
from luigi.util import inherits, copies

from b2luigi.core.parameter import wrap_parameter, BoolParameter

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
            if not hasattr(task_that_requires, param_name) and not param_name in self.kwargs:
                # If not, add it to the inheriting task
                setattr(task_that_requires, param_name, param_obj)

        old_requires = task_that_requires.requires

        # Modify task_that_requres by adding methods
        def requires(_self):
            yield from old_requires(_self)
            yield _self.clone(cls=self.task_to_require, **self.kwargs)

        task_that_requires.requires = requires

        return task_that_requires
