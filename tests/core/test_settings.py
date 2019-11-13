import json
import os
import warnings

from ..helpers import B2LuigiTestCase

import b2luigi
from b2luigi.core.settings import DeprecatedSettingsWarning


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

        self.assertEqual("my file value", b2luigi.get_setting("my_setting", task=task))
        self.assertEqual("my value", b2luigi.get_setting("my_second_setting", task=task))
        self.assertEqual("my task value", b2luigi.get_setting("my_third_setting", task=task))
        

    def test_deprecated_settings(self):
        self.assertRaises(ValueError, b2luigi.get_setting, key="my_setting", 
                          deprecated_keys=["my_old_setting"])

        b2luigi.set_setting("my_old_setting", "my value")

        with warnings.catch_warnings(record=True) as w:
            self.assertEqual("my value", b2luigi.get_setting("my_setting", 
                            deprecated_keys=["my_old_setting"]))

            self.assertEqual(len(w), 1)
            self.assertIsInstance(w[-1].message, DeprecatedSettingsWarning)
            self.assertIn("deprecated", str(w[-1].message))

        b2luigi.set_setting("my_setting", "my new_value")

        with warnings.catch_warnings(record=True) as w:
            self.assertEqual("my new_value", b2luigi.get_setting("my_setting", default="default", 
                            deprecated_keys=["my_old_setting"]))

            self.assertEqual(len(w), 0)