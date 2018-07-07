"""Task scheduling and batch running for basf2 jobs made simple"""
from setuptools_scm import get_version
__version__ = get_version(root='..', relative_to=__file__)

from luigi import *

from luigi.util import requires, inherits

from b2luigi.core.tasks import Task, ExternalTask, WrapperTask, DispatchableTask, ROOTLocalTarget
from b2luigi.core.helper_tasks import Basf2Task, SimplifiedOutputBasf2Task, Basf2FileMergeTask, HaddTask
from b2luigi.cli.process import process
