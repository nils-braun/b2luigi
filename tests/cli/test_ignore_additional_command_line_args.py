from ..helpers import B2LuigiTestCase


class TestIgnoreAdditionalCommandLineArgs(B2LuigiTestCase):
    def test_accept_user_args_if_ignored_by_b2luigi(self):
        """
        Check that we can use our own CLI args in scripts if
        ``ignore_additional_command_line_args`` is ``True``, which means they
        should be ignore by ``b2luigi.process``.
        """
        self.call_file(
            "cli/process_with_user_args_ignored_by_b2luigi.py", cli_args=["--hello", "world", "1", "2"]
        )
