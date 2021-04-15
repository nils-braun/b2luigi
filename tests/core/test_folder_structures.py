import os

from ..helpers import B2LuigiTestCase


class TemporaryWrapperTestCase(B2LuigiTestCase):
    def test_result_dir(self):
        self.call_file("core/folder_structures.py")

        self.assertTrue(os.path.exists("results/test.txt"))

        with open("results/test.txt", "r") as f:
            self.assertEqual(f.read(), "Test")

    def test_result_dir_from_other_location(self):
        os.makedirs("some_other_folder", exist_ok=True)
        self.call_file("core/folder_structures.py", cwd="some_other_folder")

        self.assertTrue(os.path.exists("results/test.txt"))
        self.assertTrue(os.path.exists("some_other_folder"))
        self.assertFalse(os.path.exists("some_other_folder/results/test.txt"))

        with open("results/test.txt", "r") as f:
            self.assertEqual(f.read(), "Test")
