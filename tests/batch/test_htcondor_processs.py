"""
Test helper functions for :py:class:`Gbasf2Process`.

Utilities that require a running gbasf2 or Dirac environment will not net
tested in this test case, only the functions that can be run independently.
"""

from b2luigi.batch.processes.htcondor import HTCondorProcess

from ..helpers import B2LuigiTestCase
from .batch_task_1 import MyTask
from unittest.mock import Mock
import os
import b2luigi


class TestHTCondorCreateSubmitFile(B2LuigiTestCase):

    def _get_htcondor_submit_file_string(self, task):
        # the _create_htcondor_submit_file is a method of the ``HTCondorProcess`` class, but it only uses its
        # class to obtain ``self.task``, to it's sufficient to provide a mock class for ``self``, `
        htcondor_mock_process = Mock()
        task.get_task_file_dir = lambda: self.test_dir
        htcondor_mock_process.task = task
        #  create submit file
        HTCondorProcess._create_htcondor_submit_file(htcondor_mock_process)
        # read submit file and return string
        submit_file_path = os.path.join(self.test_dir, "job.submit")
        with open(submit_file_path, "r") as submit_file:
            return submit_file.read()

    def test_not_setting_job_name(self):
        submit_file_string = self._get_htcondor_submit_file_string(MyTask("some_parameter"))
        self.assertNotIn("JobBatchName", submit_file_string)

    def test_set_job_name_via_task_attribute(self):
        task = MyTask("some_parameter")
        task.job_name = "some_job_name"
        submit_file_lines = self._get_htcondor_submit_file_string(task).splitlines()
        self.assertIn("JobBatchName = some_job_name", submit_file_lines)

        b2luigi.set_setting("job_name", "some_job_name")
        submit_file_lines = self._get_htcondor_submit_file_string(MyTask("some_parameter")).splitlines()
        b2luigi.clear_setting("job_name")
        self.assertIn("JobBatchName = some_job_name", submit_file_lines)

    def test_set_job_name_is_overriden_by_htcondor_settings(self):
        """
        ``job_name`` is a global setting, but if the ``JobBatchName`` is set explicitly via the settings, we
        want that to override the global setting
        """
        task = MyTask("some_parameter")
        task.job_name = "job_name_global"
        htcondor_settings = {"JobBatchName": "job_name_htcondor"}
        b2luigi.set_setting("htcondor_settings", htcondor_settings)
        submit_file_lines = self._get_htcondor_submit_file_string(task).splitlines()
        b2luigi.clear_setting("htcondor_settings")
        self.assertNotIn("JobBatchName = job_name_global", submit_file_lines)
        self.assertIn("JobBatchName = job_name_htcondor", submit_file_lines)
