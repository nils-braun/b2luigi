import os
import subprocess
import sys
from glob import glob

from ..helpers import B2LuigiTestCase


class ExampleTestCase(B2LuigiTestCase):
    pass


for file_name in glob(os.path.join(os.path.dirname(__file__), "*.py")):
    def lambda_function(self):
        subprocess.check_call([sys.executable, os.path.abspath(file_name)])

    short_file_name = os.path.basename(file_name)
    short_file_name = os.path.splitext(short_file_name)[0]

    if short_file_name.startswith("test_") or short_file_name.startswith("__"):
        continue

    setattr(ExampleTestCase, "test_" + short_file_name, lambda_function)