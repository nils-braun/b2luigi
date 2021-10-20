import argparse

import b2luigi


class AlwaysExistingTarget(b2luigi.Target):
    """
    Target that always exists
    """

    def exists(self):
        return True


class DoNothingTask(b2luigi.ExternalTask):
    """
    Dummy task that is always already complete and needs nothing to be done.

    Useful for testing ``b2luigi.process`` just for its CLI argument parsing
    with some dummy task.
    """

    def output(self):
        return AlwaysExistingTarget()


if __name__ == "__main__":
    # add some dummy user args. They should work with the ignore_additional_command_line_args option
    parser = argparse.ArgumentParser("")
    parser.add_argument("--hello", help="Keyword argument, should be ``'world'``")
    parser.add_argument("is_one", help="First positional argument, should be ``1``")
    parser.add_argument("is_two", help="Second positional argument, should be ``2``")
    args = parser.parse_args()
    # check that none of the args is None
    assert args.hello == "world", "--hello should be `world`"
    assert int(args.is_one) == 1, "first positional arg should be 1"
    assert int(args.is_two) == 2, "second positional arg should be 2"
    # process task that does nothing. b2luigi.process parses its own cli args,
    # but with the ignore option that should not result in collisions
    b2luigi.process(DoNothingTask(), ignore_additional_command_line_args=True)
