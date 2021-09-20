from subprocess import CalledProcessError
from ..helpers import B2LuigiTestCase


class TestIgnoreAdditionalCommandLineArgs(B2LuigiTestCase):
    def test_accept_user_args_if_ignored_by_b2luigi(self):
        """
        Check that we can use our own CLI args in scripts if
        ``ignore_additional_command_line_args`` is ``True``, which means they
        should be ignore by ``b2luigi.process``.
        """
        self.call_file(
            "cli/process_with_user_args_ignored_by_b2luigi.py",
            cli_args=["--hello", "world", "1", "2"],
        )

    def test_user_args_when_not_ignored_raises_error(self):
        """
        Check that we can't use undefined CLI args in scripts if
        ``ignore_additional_command_line_args=False`` in ``b2luigi.process.
        """
        with self.assertRaises(CalledProcessError):
            self.call_file(
                "cli/process_with_user_args_not_ignored_by_b2luigi.py",
                cli_args=["--hello", "world"],
            )
