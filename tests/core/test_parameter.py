from unittest import TestCase

import b2luigi

from ..helpers import B2LuigiTestCase


class HashedParameterTestCase(B2LuigiTestCase):
    def test_hash_consistency(self):
        first_parameter = b2luigi.DictParameter()
        self.assertFalse(hasattr(first_parameter, "serialize_hashed"))

        second_parameter = b2luigi.DictParameter(hashed=True)
        self.assertTrue(hasattr(second_parameter, "serialize_hashed"))

        serialized_first = second_parameter.serialize_hashed({"key1": "value1", "key2": 12345})
        serialized_second = second_parameter.serialize_hashed({"key1": "value1", "key2": 12345})

        self.assertEqual(serialized_first, serialized_second)
        self.assertEqual(serialized_first, "hashed_df6221c515cbb93735f9478cb05a00e4")

        serialized_first = second_parameter.serialize_hashed([1, "test", 456, {"hello": "bye"}])
        serialized_second = second_parameter.serialize_hashed([1, "test", 457, {"hello": "bye"}])

        self.assertNotEqual(serialized_first, serialized_second)
        self.assertEqual(serialized_first, "hashed_7816c14282fd03e3dc4e398f28aa5a30")

    def test_with_task(self):
        class MyTask(b2luigi.Task):
            my_parameter = b2luigi.ListParameter(hashed=True)

            def run(self):
                with open(self.get_output_file_name("test.txt"), "w") as f:
                    f.write("test")

            def output(self):
                yield self.add_to_output("test.txt")

        task = MyTask(my_parameter=["Some", "strange", "items", "with", "bad / signs"])

        self.assertEqual(task.get_output_file_name("test.txt"),
                         "results/my_parameter=hashed_08928069d368e4a0f8ac02a0193e443b/test.txt")
