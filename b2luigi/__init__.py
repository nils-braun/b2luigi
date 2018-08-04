"""Task scheduling and batch running for basf2 jobs made simple"""
__version__ = "0.2.0"

from luigi import *

from luigi.util import requires, inherits

from b2luigi.core.task import Task, ExternalTask, WrapperTask
from b2luigi.core.temporary_wrapper import on_temporary_files
from b2luigi.core.dispatchable_task import DispatchableTask, dispatch
from b2luigi.core.settings import get_setting, set_setting, clear_setting
from b2luigi.cli.process import process
