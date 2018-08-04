import os
import subprocess

from ..helpers import B2LuigiTestCase

import b2luigi


FIRST_CONTENT = """
import b2luigi

class MyTask(b2luigi.Task):
    def output(self):
        yield self.add_to_output("test.txt")

    @b2luigi.on_temporary_files
    def run(self):
        with open(self.get_output_file_name("test.txt"), "w") as f:
            raise ValueError()
            f.write("Test")

if __name__ == "__main__":
    b2luigi.process(MyTask())
"""

SECOND_CONTENT = """
import b2luigi

class MyTask(b2luigi.Task):
    def output(self):
        yield self.add_to_output("test.txt")

    @b2luigi.on_temporary_files
    def run(self):
        with open(self.get_output_file_name("test.txt"), "w") as f:
            f.write("Test")

if __name__ == "__main__":
    b2luigi.process(MyTask())
"""


class TemporaryWrapperTestCase(B2LuigiTestCase):
    def test_failed_temporary_files(self):
        with open("test.py", "w") as f:
            f.write(FIRST_CONTENT)

        subprocess.check_call(["python3", "test.py"])

        self.assertFalse(os.path.exists("test.txt"))

    def test_good_temporary_files(self):
        with open("test.py", "w") as f:
            f.write(SECOND_CONTENT)

        subprocess.check_call(["python3", "test.py"])

        self.assertTrue(os.path.exists("test.txt"))

        with open("test.txt", "r") as f:
            self.assertEqual(f.read(), "Test")

    def test_reset_output(self):

        class MyTask(b2luigi.Task):
            def output(self):
                yield self.add_to_output("test.txt")

            @b2luigi.on_temporary_files
            def run(self):
                with open(self.get_output_file_name("test.txt"), "w") as f:
                    f.write("Test")

        task = MyTask()

        self.assertNotIn("tmp", task.get_output_file_name("test.txt"))