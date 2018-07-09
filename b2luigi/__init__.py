"""Task scheduling and batch running for basf2 jobs made simple"""
__version__ = "0.1.2"

from luigi import *

from luigi.util import requires, inherits

from b2luigi.core.task import Task, ExternalTask, WrapperTask
from b2luigi.core.dispatchable_task import DispatchableTask
from b2luigi.cli.process import process
