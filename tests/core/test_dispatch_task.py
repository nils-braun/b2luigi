import subprocess

from ..helpers import B2LuigiTestCase

import os


class DispatchTaskTestCase(B2LuigiTestCase):
    def test_failing_task_class(self):
        for file_name in ["core/dispatch_1.py", "core/dispatch_2.py"]:
            out = self.call_file(file_name, stderr=subprocess.STDOUT)

            self.assertTrue(os.path.exists("results/some_file.txt"))
            self.assertFalse(os.path.exists("results/some_other_file.txt"))
            self.assertTrue(os.path.exists("logs/MyTask/stderr"))
            self.assertTrue(os.path.exists("logs/MyTask/stdout"))

            with open("logs/MyTask/stdout", "r") as f:
                self.assertEqual(f.readlines(), ["Hello!\n", "Bye!\n"])

            self.assertIn(b"Task MyTask failed!", out.splitlines())
            self.assertIn(b"Please have a look into the log files in", out.splitlines())
            self.assertIn(b"RuntimeError: Execution failed with return code -11", out.splitlines())