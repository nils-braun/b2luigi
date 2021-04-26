import json
import os
import tempfile
from typing import List
from unittest.mock import MagicMock, Mock, patch

import b2luigi
from b2luigi.batch.processes.gbasf2 import Gbasf2Process, JobStatus, get_unique_project_name

from ..helpers import B2LuigiTestCase
from .batch_task_1 import MyTask


class MyGbasf2Task(MyTask):
    gbasf2_project_name_prefix = "my_gb2_task_"


class TestGbasf2FailedFilesDownload(B2LuigiTestCase):

    download_stdouts_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), "_gbasf2_project_download_stdouts")

    def setUp(self):
        super().setUp()
        self.gb2_mock_process = Mock()
        self.gb2_mock_process.task = MyGbasf2Task("some_parameter")
        self.gb2_mock_process.dirac_user = "username"
        self.gb2_mock_process.gbasf2_project_name = get_unique_project_name(self.gb2_mock_process.task)
        self.gb2_mock_process.max_retries = 0
        b2luigi.set_setting("gbasf2_print_status_updates", False)

    def _get_download_stdout(self, download_stdout_name):
        with open(os.path.join(self.download_stdouts_dir, download_stdout_name)) as download_stdout_file:
            return download_stdout_file.read()

    def assert_failed_files(self, download_stdout_name, expected_number_of_failed_files):
            failed_files = Gbasf2Process._failed_files_from_dataset_download(self.gb2_mock_process,
                                                                             self._get_download_stdout(download_stdout_name))
            self.assertEqual(len(failed_files), expected_number_of_failed_files)

    def test_failed_files_all_successful(self):
        "Test gbasf2 project download output where all downloads are successful"
        self.assert_failed_files("all_successful.txt", 0)

    def test_failed_files_all_successful_and_duplicate(self):
        "Test gbasf2 project download output where all downloads are successful, and duplicates available"
        self.assert_failed_files("all_successful_and_duplicate.txt", 0)

    def test_failed_files_failed_and_successful_and_duplicate(self):
        "Test gbasf2 project download output where downloads are failed, successful, and duplicates available"
        self.assert_failed_files("failed_and_successful_and_duplicate.txt", 2)

    def test_failed_files_failed_and_successful(self):
        "Test gbasf2 project download output where downloads are failed and successful"
        self.assert_failed_files("failed_and_successful.txt", 2)

    def test_failed_files_all_failed(self):
        "Test gbasf2 project download output where all downloads are failed"
        self.assert_failed_files("all_failed.txt", 3)

    def test_failed_files_all_failed_and_duplicate(self):
        "Test gbasf2 project download output where all downloads are failed"
        self.assert_failed_files("all_failed_and_duplicate.txt", 3)


class TestGbasf2GetJobStatus(B2LuigiTestCase):

    job_statuses_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), "_gbasf2_project_statuses")

    def setUp(self):
        super().setUp()
        self.gb2_mock_process = Mock()
        self.gb2_mock_process.task = MyGbasf2Task("some_parameter")
        self.gb2_mock_process.dirac_user = "username"
        self.gb2_mock_process.gbasf2_project_name = get_unique_project_name(self.gb2_mock_process.task)
        self.gb2_mock_process.max_retries = 0
        b2luigi.set_setting("gbasf2_print_status_updates", False)

    def _get_job_status_dict(self, job_status_fname):
        with open(os.path.join(self.job_statuses_dir, job_status_fname)) as job_status_json_file:
            return json.load(job_status_json_file)

    def assert_job_status(self, job_status_fname, expected_job_status):
        with patch("b2luigi.batch.processes.gbasf2.get_gbasf2_project_job_status_dict",
                   MagicMock(return_value=self._get_job_status_dict(job_status_fname))):
            job_status = Gbasf2Process.get_job_status(self.gb2_mock_process)
            self.assertEqual(job_status, expected_job_status)

    def test_get_job_status_all_done(self):
        "Test gbasf2 project status dict where all jobs are done"
        self.assert_job_status("done_testjbucket1357828d80b3.json", JobStatus.successful)

    def test_get_job_status_one_failed(self):
        "Test gbasf2 project status dict where one job in project failed"
        self.assert_job_status("failed_7663_r03743_10_prod00013766_11x1.json", JobStatus.aborted)

    def test_get_job_status_running(self):
        "Test gbasf2 project status dict where several jobs are either running or waiting"
        self.assert_job_status("running_TrainingFEI_17-04-2021a10a957289.json", JobStatus.running)

    def test_get_job_status_major_status_done_but_minor_status_not(self):
        "Test gbasf2 project status dict where major job status is all done but application status is not ``DONE``"
        self.assert_job_status("all_done_but_application_error.json", JobStatus.aborted)


class TestBuildGbasf2SubmitCommand(B2LuigiTestCase):
    dummy_lfn = "/grid/path/to/dataset"

    def setUp(self):
        super().setUp()
        self.dummy_lfn_file = tempfile.NamedTemporaryFile()

    def _get_task_file_dir(self, task):
        return self.test_dir

    def _build_gbasf2_submit_command(self, task) -> List[str]:
        gb2_mock_process = Mock()
        os.makedirs(self._get_task_file_dir(task), exist_ok=True)
        self.pickle_file_path = os.path.join(self._get_task_file_dir(task), "serialized_basf2_path.pkl")
        self.wrapper_file_path = os.path.join(self._get_task_file_dir(task), "steering_file_wrapper.py")
        gb2_mock_process.pickle_file_path = self.test_dir
        gb2_mock_process.wrapper_file_path = self.test_dir
        gb2_mock_process.gbasf2_project_name = get_unique_project_name(task)
        gb2_mock_process.task = task
        return Gbasf2Process._build_gbasf2_submit_command(gb2_mock_process)

    def test_input_dataset_in_command(self):
        "Test that if the input dataset is set, it occurs once in the command string"
        class _Task(MyGbasf2Task):
            gbasf2_input_dataset = b2luigi.Parameter(default=self.dummy_lfn)
        gb2_cmd = self._build_gbasf2_submit_command(_Task("some_parameter"))
        gb2_cmd_str = " ".join(gb2_cmd)
        self.assertEqual(gb2_cmd.count(self.dummy_lfn), 1)
        self.assertEqual(gb2_cmd.count("-i"), 1)
        self.assertEqual(gb2_cmd_str.count(f" -i {self.dummy_lfn}"), 1)
        self.assertNotIn("--input_dslist", gb2_cmd_str)

    def test_input_dslist_in_command(self):
        "Test that if the input dataset list is set, it occurs once in the command string"
        class _Task(MyGbasf2Task):
            gbasf2_input_dslist = self.dummy_lfn_file.name
        gb2_cmd = self._build_gbasf2_submit_command(_Task("some_parameter"))
        gb2_cmd_str = " ".join(gb2_cmd)
        self.assertEqual(gb2_cmd.count(self.dummy_lfn_file.name), 1)
        self.assertEqual(gb2_cmd.count("--input_dslist"), 1)
        self.assertEqual(gb2_cmd_str.count(f" --input_dslist {self.dummy_lfn_file.name}"), 1)
        self.assertNotIn("-i", gb2_cmd)

    def test_input_dslist_set_but_does_not_exist(self):
        """Test that if the input dataset list is set but the file does not exist, an ``FileNotFoundError`` is raised"""
        class _Task(MyGbasf2Task):
            gbasf2_input_dslist = self.dummy_lfn_file.name
        self.dummy_lfn_file.close()
        self.assertRaises(FileNotFoundError, lambda: self._build_gbasf2_submit_command(_Task("some_parameter")))

    def test_not_dataset_raises_error(self):
        """Test that we get an error when no gbasf2 input dataset is provided"""
        self.assertRaises(RuntimeError, lambda: self._build_gbasf2_submit_command(MyGbasf2Task("some_parameter")))

    def test_both_input_dataset_and_dslist_raises_error(self):
        """Test that en error is raise when both gbasf2_input_dataset and gbasf2_inputdslist settings are given"""
        class _Task(MyGbasf2Task):
            gbasf2_input_dataset = b2luigi.Parameter(default=self.dummy_lfn)
            gbasf2_input_dslist = self.dummy_lfn_file.name
        self.assertRaises(RuntimeError, lambda: self._build_gbasf2_submit_command(MyGbasf2Task("some_parameter")))
