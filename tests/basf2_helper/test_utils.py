import os
from unittest import TestCase
from unittest.mock import patch

from b2luigi.basf2_helper.utils import get_basf2_git_hash


class TestGetBasf2GitHash(TestCase):
    def test_return_no_set(self):
        """
        On CI systems no basf2 is installed or set up, so ``get_basf2_git_hash``
        should return \"not set\".
        """
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
