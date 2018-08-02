from ..helpers import B2LuigiTestCase

import b2luigi

import os
import tempfile


class DispatchTaskTestCase(B2LuigiTestCase):

    def test_failing_task(self):
        import subprocess

        with open("dispatch_task.py", "w") as f:
            f.write("""
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
    """)

        out = subprocess.check_call(["python3", "dispatch_task.py"])

        self.assertTrue(os.path.exists("results/some_file.txt"))
        self.assertFalse(os.path.exists("results/some_other_file.txt"))
        self.assertTrue(os.path.exists("logs/MyTask_stderr"))
        self.assertTrue(os.path.exists("logs/MyTask_stdout"))

        with open("logs/MyTask_stdout", "r") as f:
            self.assertEqual(f.readlines(), ["Hello!\n", "Bye!\n"])
