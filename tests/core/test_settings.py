import json
import os

from ..helpers import B2LuigiTestCase

import b2luigi


class TaskTestCase(B2LuigiTestCase):
    def setUp(self):
        super().setUp()

        b2luigi.clear_setting("my_setting")

    def test_set_by_function(self):
        self.assertRaises(ValueError, b2luigi.get_setting, "my_setting")

        self.assertEqual("default", b2luigi.get_setting("my_setting", "default"))

        b2luigi.set_setting("my_setting", "my value")

        self.assertEqual("my value", b2luigi.get_setting("my_setting"))
        self.assertEqual("my value", b2luigi.get_setting("my_setting", "default"))

    def test_set_by_file(self):
        with open("settings.json", "w") as f:
            json.dump({"my_setting": "my file value"}, f)

        self.assertEqual("my file value", b2luigi.get_setting("my_setting"))

        b2luigi.set_setting("my_setting", "my value")

        self.assertEqual("my value", b2luigi.get_setting("my_setting"))

    def test_set_by_parent_file(self):
        os.mkdir("child_path")
        os.chdir("child_path")

        with open("../settings.json", "w") as f:
            json.dump({"my_setting": "my parent value"}, f)

        self.assertEqual("my parent value", b2luigi.get_setting("my_setting"))

        with open("settings.json", "w") as f:
            json.dump({"my_setting": "my child value"}, f)

        self.assertEqual("my child value", b2luigi.get_setting("my_setting"))

    def test_set_by_task_or_file(self):
        with open("settings.json", "w") as f:
            json.dump({"my_setting": "my file value"}, f)

        b2luigi.set_setting("my_second_setting", "my value")

        task = b2luigi.Task()
        setattr(task, "my_third_setting", "my task value")

        self.assertEqual("my file value", b2luigi.get_task_setting("my_setting", task=task))
        self.assertEqual("my value", b2luigi.get_task_setting("my_second_setting", task=task))
        self.assertEqual("my task value", b2luigi.get_task_setting("my_third_setting", task=task))
        