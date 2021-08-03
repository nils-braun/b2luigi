import os
from unittest import TestCase, mock

from b2luigi.core import utils


class ProductDictTestCase(TestCase):
    def test_basic_usage(self):
        kwargs = list(utils.product_dict(first_arg=[1, 2, 3], second_arg=["a", "b"]))

        self.assertEqual(len(kwargs), 6)

        self.assertIn({"first_arg": 1, "second_arg": "a"}, kwargs)
        self.assertIn({"first_arg": 1, "second_arg": "b"}, kwargs)
        self.assertIn({"first_arg": 2, "second_arg": "a"}, kwargs)
        self.assertIn({"first_arg": 2, "second_arg": "b"}, kwargs)
        self.assertIn({"first_arg": 3, "second_arg": "a"}, kwargs)
        self.assertIn({"first_arg": 3, "second_arg": "b"}, kwargs)

        kwargs = list(utils.product_dict(first_arg=[1, 2, 3]))

        self.assertEqual(len(kwargs), 3)

        self.assertIn({"first_arg": 1}, kwargs)
        self.assertIn({"first_arg": 2}, kwargs)
        self.assertIn({"first_arg": 3}, kwargs)

        kwargs = list(utils.product_dict(first_arg=[1, 2, 3], second_arg=[]))

        self.assertEqual(len(kwargs), 0)

        kwargs = list(utils.product_dict(first_arg=[1], second_arg=["a"]))

        self.assertEqual(len(kwargs), 1)

        self.assertIn({"first_arg": 1, "second_arg": "a"}, kwargs)


class FlattenTestCase(TestCase):
    def test_list_input(self):
        inputs = [{"key1": "value1"}, {"key2": "value2"}]

        outputs = utils.flatten_to_dict(inputs)

        self.assertIn("key1", outputs)
        self.assertEqual(outputs["key1"], "value1")
        self.assertIn("key2", outputs)
        self.assertEqual(outputs["key2"], "value2")

        inputs = [{"key1": "value1"}, {"key2": "value2"}, {"key1": "repeated"}]

        outputs = utils.flatten_to_dict(inputs)

        self.assertIn("key1", outputs)
        self.assertEqual(outputs["key1"], "repeated")
        self.assertIn("key2", outputs)
        self.assertEqual(outputs["key2"], "value2")

        inputs = []

        outputs = utils.flatten_to_dict(inputs)

        self.assertFalse(outputs)

        inputs = [{"key1": "value1"}, "value2"]

        outputs = utils.flatten_to_dict(inputs)

        self.assertIn("key1", outputs)
        self.assertEqual(outputs["key1"], "value1")
        self.assertIn("value2", outputs)
        self.assertEqual(outputs["value2"], "value2")

        inputs = ["value1", "value2"]

        outputs = utils.flatten_to_dict(inputs)

        self.assertIn("value1", outputs)
        self.assertEqual(outputs["value1"], "value1")
        self.assertIn("value2", outputs)
        self.assertEqual(outputs["value2"], "value2")

    def test_raw_input(self):
        inputs = {"key1": "value1"}

        outputs = utils.flatten_to_dict(inputs)

        self.assertIn("key1", outputs)
        self.assertEqual(outputs["key1"], "value1")

        inputs = "value1"

        outputs = utils.flatten_to_dict(inputs)

        self.assertIn("value1", outputs)
        self.assertEqual(outputs["value1"], "value1")

    def test_list_of_list_input(self):
        inputs = [{"key1": "value1"}, {"key2": "value2"}]

        outputs = utils.flatten_to_list_of_dicts(inputs)

        self.assertIn("key1", outputs)
        self.assertEqual(outputs["key1"], ["value1"])
        self.assertIn("key2", outputs)
        self.assertEqual(outputs["key2"], ["value2"])

        inputs = [{"key1": "value1"}, {"key2": "value2"}, {"key1": "repeated"}]

        outputs = utils.flatten_to_list_of_dicts(inputs)

        self.assertIn("key1", outputs)
        self.assertEqual(outputs["key1"], ["value1", "repeated"])
        self.assertIn("key2", outputs)
        self.assertEqual(outputs["key2"], ["value2"])

        inputs = []

        outputs = utils.flatten_to_list_of_dicts(inputs)

        self.assertFalse(outputs)

        inputs = [{"key1": "value1"}, "value2"]

        outputs = utils.flatten_to_list_of_dicts(inputs)

        self.assertIn("key1", outputs)
        self.assertEqual(outputs["key1"], ["value1"])
        self.assertIn("value2", outputs)
        self.assertEqual(outputs["value2"], ["value2"])

        inputs = ["value1", "value2"]

        outputs = utils.flatten_to_list_of_dicts(inputs)

        self.assertIn("value1", outputs)
        self.assertEqual(outputs["value1"], ["value1"])
        self.assertIn("value2", outputs)
        self.assertEqual(outputs["value2"], ["value2"])

        inputs = [[{"key1": "value1"}, {"key2": "value2"}], [{"key1": "repeated"}]]

        outputs = utils.flatten_to_list_of_dicts(inputs)

        self.assertIn("key1", outputs)
        self.assertEqual(outputs["key1"], ["value1", "repeated"])
        self.assertIn("key2", outputs)
        self.assertEqual(outputs["key2"], ["value2"])


class MapFolderTestCase(TestCase):
    dummy_rel_dir = "./some/rel_dir"
    dummy_abs_dir = "/path/to/some/abs_dir"
    main_no_file_file_err_msg = "module '__main__' has no attribute '__file__'"

    def test_map_folder_abspath_identity(self):
        """Test that for an absolute path, map_folder returns and identity"""
        self.assertEqual(utils.map_folder(self.dummy_abs_dir), self.dummy_abs_dir)

    def test_map_folder_relpath(self):
        """
        Test map_folder with a relative input_folder, which joins it with ``__main__.__file__``
        """
        with mock.patch("__main__.__file__", self.dummy_abs_dir):
            mapped_folder = utils.map_folder(self.dummy_rel_dir)
            self.assertEqual(
                mapped_folder, os.path.join(self.dummy_abs_dir, mapped_folder)
            )

    def test_map_folder_abspath_identity_when_no_filename(self):
        """
        Test that for an absolute path, map_folder returns and identity even
        if ``get_filename`` would raise an ``AttributeError`` because ``__main__.__file__``
        is not available (e.g. in jupyter)
        """
        with mock.patch(
            "b2luigi.core.utils.get_filename",
            side_effect=AttributeError(self.main_no_file_file_err_msg),
        ):
            mapped_folder = utils.map_folder(self.dummy_abs_dir)
            self.assertEqual(mapped_folder, self.dummy_abs_dir)

    def test_map_folder_raises_attribute_error_for_relpath_when_no_filename(self):
        """
        Test that when ``get_filename`` returns an ``AttributeError`` b/c
        ``__main__.__file__`` is not available, ``map_folder`` also returns an
        ``AttributeError`` when the input folder is relative
        """
        with self.assertRaises(AttributeError):
            with mock.patch(
                "b2luigi.core.utils.get_filename",
                side_effect=AttributeError(self.main_no_file_file_err_msg),
            ):
                utils.map_folder(self.dummy_rel_dir)

    def _get_map_folder_error_mesage(self):
        """
        Get the error message that ``map_folder`` raises when ``get_filename``
        raises an attribute error because ``__main__.__file__`` is not
        accessible (e.g. in Jupyter)
        """
        try:
            with mock.patch(
                "b2luigi.core.utils.get_filename",
                side_effect=AttributeError(self.main_no_file_file_err_msg),
            ):
                utils.map_folder(self.dummy_rel_dir)
        except AttributeError as err:
            return str(err)
        raise RuntimeError("No AttributeError raised when calling ``utils.map_folder``")

    def test_original_message_in_error(self):
        """
        Check that the error message of ``map_folder`` still contains the
        original error message raised by ``get_filename`` during an
        ``AttributeError`` due to ``__main__.__file__`` not being accessible
        """
        message = self._get_map_folder_error_mesage()
        self.assertTrue(message.endswith(self.main_no_file_file_err_msg))

    def test_additional_info_added_to_error(self):
        """
        Check that the error message of ``map_folder`` adds additional
        information to the ``AttributeError`` raised by ``get_filename``
        """
        message = self._get_map_folder_error_mesage()
        self.assertTrue(message.startswith("Could not determine the current script location. "))
