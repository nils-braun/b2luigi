import os
from typing import List
from unittest.mock import Mock

import b2luigi
from b2luigi.batch.processes.gbasf2 import Gbasf2Process, get_unique_project_name

from ..helpers import B2LuigiTestCase
from .batch_task_1 import MyTask
import shlex
import tempfile


class MyGbasf2Task(MyTask):
    gbasf2_project_name_prefix = "my_gb2_task_"


class testBuildGbasf2SubmitCommand(B2LuigiTestCase):
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
        gb2_cmd_str = shlex.join(gb2_cmd)
        self.assertEqual(gb2_cmd.count(self.dummy_lfn), 1)
        self.assertEqual(gb2_cmd.count("-i"), 1)
        self.assertEqual(gb2_cmd_str.count(f" -i {self.dummy_lfn}"), 1)
        self.assertNotIn("--input_dslist", gb2_cmd_str)

    def test_input_dslist_in_command(self):
        "Test that if the input dataset list is set, it occurs once in the command string"
        class _Task(MyGbasf2Task):
            gbasf2_input_dslist = self.dummy_lfn_file.name
        gb2_cmd = self._build_gbasf2_submit_command(_Task("some_parameter"))
        gb2_cmd_str = shlex.join(gb2_cmd)
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
