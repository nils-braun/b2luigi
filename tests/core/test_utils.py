from unittest import TestCase

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