from unittest import TestCase

from b2luigi.core import parameter


class HashedParameterTestCase(TestCase):
    def test_hash_consistency(self):
        first_parameter = parameter.HashedParameter()

        serialized_first = first_parameter.serialize({"key1": "value1", "key2": 12345})
        serialized_second = first_parameter.serialize({"key1": "value1", "key2": 12345})

        self.assertEqual(serialized_first, serialized_second)
        self.assertEqual(serialized_first, "hashed_df6221c515cbb93735f9478cb05a00e4")

        serialized_first = first_parameter.serialize([1, "test", 456, {"hello": "bye"}])
        serialized_second = first_parameter.serialize([1, "test", 457, {"hello": "bye"}])

        self.assertNotEqual(serialized_first, serialized_second)
        self.assertEqual(serialized_first, "hashed_7816c14282fd03e3dc4e398f28aa5a30")