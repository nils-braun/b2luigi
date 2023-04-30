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
    b2luigi.process(DoNothingTask(), ignore_additional_command_line_args=False)
