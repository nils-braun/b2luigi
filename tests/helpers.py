import b2luigi

from unittest import TestCase
import tempfile
import os
import shutil

class B2LuigiTestCase(TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        os.chdir(self.test_dir)

        b2luigi.set_setting("result_path", "results")

    def tearDown(self):
        shutil.rmtree(self.test_dir)