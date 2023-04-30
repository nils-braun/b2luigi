"""
Test helper functions for :py:class:`Gbasf2Process`.

Utilities that require a running gbasf2 or Dirac environment will not net
tested in this test case, only the functions that can be run independently.
"""

import json
import os
import subprocess
import unittest
from unittest import mock

import b2luigi
from b2luigi.batch.processes.htcondor import HTCondorJobStatusCache, HTCondorProcess

from ..helpers import B2LuigiTestCase
from .batch_task_1 import MyTask


class TestHTCondorCreateSubmitFile(B2LuigiTestCase):
    def _get_htcondor_submit_file_string(self, task):
        # the _create_htcondor_submit_file is a method of the ``HTCondorProcess`` class, but it only uses its
        # class to obtain ``self.task``, to it's sufficient to provide a mock class for ``self``, `
        htcondor_mock_process = mock.Mock()
        task.get_task_file_dir = lambda: self.test_dir
        htcondor_mock_process.task = task
        #  create submit file
        HTCondorProcess._create_htcondor_submit_file(htcondor_mock_process)
        # read submit file and return string
        submit_file_path = os.path.join(self.test_dir, "job.submit")
        with open(submit_file_path, "r") as submit_file:
            return submit_file.read()

    def test_minimal_submit_file(self):
        """
        Minimal  submit file should have expected shape:

            output = ...
            error = ..
            log = ..
            executable = executable_wrapper.sh
            queue 1
        """
        submit_file_lines = self._get_htcondor_submit_file_string(MyTask("some_parameter")).splitlines()
        self.assertIn("output = ", submit_file_lines[0])
        self.assertIn("error = ", submit_file_lines[1])
        self.assertIn("log = ", submit_file_lines[2])
        self.assertEqual("executable = executable_wrapper.sh", submit_file_lines[3])
        self.assertEqual("queue 1", submit_file_lines[4])

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


class TestHTCondorJobStatusCache(unittest.TestCase):
    def setUp(self):
        mock_status_dicts = [
            {
                "JobStatus": 4,  # completed
                "ExitCode": 0,  # success
                "ClusterId": 42,
            },
            {
                "JobStatus": 2,  # running
                "ClusterId": 43,
            },
            {
                "JobStatus": 999,  # failed
                "ClusterId": 44,
            },
        ]
        self.mock_status_json = json.dumps(mock_status_dicts).encode()

        self.htcondor_job_status_cache = HTCondorJobStatusCache()

    @mock.patch("subprocess.check_output")
    def test_ask_for_job_status_does_2_retries(self, mock_check_output):
        """Test the ``_ask_for_job_status`` recovers after two condor_q failures."""

        # make check_output fail 2 times before  return status dict
        n_fail = 2
        mock_check_output.side_effect = n_fail * [
            subprocess.CalledProcessError(1, ["mock", "command"])
        ] + [self.mock_status_json]
        self.htcondor_job_status_cache._ask_for_job_status()
        self.assertEqual(mock_check_output.call_count, 3)

    @mock.patch("subprocess.check_output")
    def test_ask_for_job_status_no_retries_on_success(self, mock_check_output):
        """Test the ``_ask_for_job_status`` is only called once (no retries) when everything works."""

        # make check_output fail 2 times before  return status dict
        mock_check_output.return_value = self.mock_status_json
        self.htcondor_job_status_cache._ask_for_job_status()
        self.assertEqual(mock_check_output.call_count, 1)

    @mock.patch("subprocess.check_output")
    def test_ask_for_job_status_fails_after_4_condor_q_failures(
        self, mock_check_output
    ):
        """Test the ``_ask_for_job_status`` does not do more than 3 retries"""

        # make check_output fail 2 times before  return status dict
        n_fail = 4
        mock_check_output.side_effect = n_fail * [
            subprocess.CalledProcessError(1, ["mock", "command"])
        ] + [self.mock_status_json]
        with self.assertRaises(subprocess.CalledProcessError):
            self.htcondor_job_status_cache._ask_for_job_status()
