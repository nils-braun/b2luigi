import os
from glob import glob

from ..helpers import B2LuigiTestCase


class ExampleTestCase(B2LuigiTestCase):
    def test_examples(self):
        for file_name in glob(os.path.join(os.path.dirname(__file__), "*.py")):
            short_file_name = os.path.basename(file_name)

            if short_file_name.startswith("test_") or short_file_name.startswith("__"):
                continue

            self.call_file(os.path.join("doc_examples/", os.path.basename(file_name)))
