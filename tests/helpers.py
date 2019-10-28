import subprocess
import sys

import b2luigi

from unittest import TestCase
import tempfile
import os
import shutil


class B2LuigiTestCase(TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.cwd = os.getcwd()
        os.chdir(self.test_dir)

        b2luigi.set_setting("result_path", "results")

    def tearDown(self):
        os.chdir(self.cwd)
        shutil.rmtree(self.test_dir)

    def call_file(self, file_name, cwd=None, **kwargs):
        if not cwd:
            cwd = self.test_dir
        test_file_name = os.path.join(os.path.dirname(__file__), file_name)
        tmp_file_name = os.path.join(self.test_dir, os.path.basename(file_name))
        shutil.copy(test_file_name, tmp_file_name)
        return subprocess.check_output([sys.executable, tmp_file_name], cwd=cwd, **kwargs)
