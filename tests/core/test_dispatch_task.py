from ..helpers import B2LuigiTestCase

import b2luigi

import os
import tempfile


FIRST_CONTENT = """
import b2luigi
import os


class MyTask(b2luigi.DispatchableTask):
    def output(self):
        yield self.add_to_output("some_file.txt")
        yield self.add_to_output("some_other_file.txt")

    def process(self):
        print("Hello!")
        with self.get_output_target("some_file.txt").open("w") as f:
            f.write("Done")

        print("Bye!")
        import sys
        sys.stdout.flush()
        os.kill(os.getpid(), 11)

        with self.get_output_target("some_other_file.txt").open("w") as f:
            f.write("Done")

if __name__ == "__main__":
    b2luigi.set_setting("result_path", "results")
    b2luigi.process(MyTask())
"""

SECOND_CONTENT = """
import b2luigi
import os


class MyTask(b2luigi.Task):
    def output(self):
        yield self.add_to_output("some_file.txt")
        yield self.add_to_output("some_other_file.txt")

    @b2luigi.dispatch
    def run(self):
        print("Hello!")
        with self.get_output_target("some_file.txt").open("w") as f:
            f.write("Done")

        print("Bye!")
        import sys
        sys.stdout.flush()
        os.kill(os.getpid(), 11)

        with self.get_output_target("some_other_file.txt").open("w") as f:
            f.write("Done")

if __name__ == "__main__":
    b2luigi.set_setting("result_path", "results")
    b2luigi.process(MyTask())
"""

class DispatchTaskTestCase(B2LuigiTestCase):
    def test_failing_task_class(self):
        import subprocess

        for content in [FIRST_CONTENT, SECOND_CONTENT]:
            with open("dispatch_task.py", "w") as f:
                f.write(content)

            out = subprocess.check_output(["python3", "dispatch_task.py"], stderr=subprocess.STDOUT)

            self.assertTrue(os.path.exists("results/some_file.txt"))
            self.assertFalse(os.path.exists("results/some_other_file.txt"))
            self.assertTrue(os.path.exists("logs/MyTask_stderr"))
            self.assertTrue(os.path.exists("logs/MyTask_stdout"))

            with open("logs/MyTask_stdout", "r") as f:
                self.assertEqual(f.readlines(), ["Hello!\n", "Bye!\n"])

            self.assertIn(b"Task MyTask failed!", out.splitlines())
            self.assertIn(b"Please have a look into the log files", out.splitlines())
            self.assertIn(b"RuntimeError: Execution failed with return code -11", out.splitlines())