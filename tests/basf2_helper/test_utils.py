import os
import sys
import warnings

from unittest import TestCase
from unittest.mock import patch

from b2luigi.basf2_helper.utils import get_basf2_git_hash


class TestGetBasf2GitHash(TestCase):
    def test_return_no_set(self):
        """
        On CI systems no basf2 is installed or set up, so ``get_basf2_git_hash``
        should return \"not set\".
        """
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=ImportWarning)
            self.assertEqual(get_basf2_git_hash(), "not_set")

    def test_return_no_set_warning(self):
        """
        On CI systems no basf2 is installed or set up, so ``get_basf2_git_hash``
        should print an ``ImportWarning``.
        """
        with self.assertWarns(ImportWarning):
            get_basf2_git_hash()

    @patch.dict(os.environ, {"BELLE2_RELEASE": "belle2_release_xy"})
    def test_belle2_release_env_is_set(self):
        self.assertEqual(get_basf2_git_hash(), "belle2_release_xy")

    def test_basf2_hash_from_get_version(self):
        "In older basf2 releases we get version from ``basf2.version.get_version``."
        class MockVersion:
            @staticmethod
            def get_version():
                return "some_basf2_version"

        class MockBasf2:
            version = MockVersion

        with patch.dict(sys.modules, {"basf2": MockBasf2, "basf2.version": MockVersion}):
            self.assertEqual(get_basf2_git_hash(), "some_basf2_version")

    def test_basf2_hash_from_version_property(self):
        "In newer basf2 releases we get version from ``basf2.version.version``."

        class MockVersion:
            version = "another_basf2_version"

        class MockBasf2:
            version = MockVersion

        with patch.dict(sys.modules, {"basf2": MockBasf2, "basf2.version": MockVersion}):
            self.assertEqual(get_basf2_git_hash(), "another_basf2_version")
