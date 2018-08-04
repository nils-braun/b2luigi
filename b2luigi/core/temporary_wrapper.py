from contextlib import ExitStack
from functools import wraps


class TemporaryFileContextManager(ExitStack):
    def __init__(self, task):
        super().__init__()

        self._task = task
        self._task_output_function = task.get_output_file_name

        self._open_files = {}

    def __enter__(self):
        def get_output_file_name(key):
            if key not in self._open_files:
                target = self._task._get_output_target(key)
                temporary_path = target.temporary_path()
                self._open_files[key] = self.enter_context(temporary_path)

            return self._open_files[key]

        self._task.get_output_file_name = get_output_file_name

    def __exit__(self, *exc_details):
        super().__exit__(*exc_details)

        self._task.get_output_file_name = self._task_output_function


def on_temporary_files(run_function):
    """
    Wrapper for decorating a task's run function to use temporary files as outputs.

    A common problem when using long running tasks in luigi is the thanksgiving bug.
    It occurs, when you define an output of a task and in its run function
    you create this output and fill it with content during a long lasting calculation.
    It may happen, that during the creation of the output and the finish of the calculation
    some other tasks look if the output is already there, find it existing and assume,
    that the task is already finished (although there is probably only nonsense in the file).

    A solution is already given by luigi itself, when using the temporary_path() function
    of the file system targets, which is really nice!
    Unfortunately, this means you have to open all your output files with a context manager
    and this is very hard to do if you have external tasks also (because they will
    probably use the output file directly instead of the temporary file version of if).

    This wrapper simplifies the usage of the temporary files::

        import b2luigi

        class MyTask(b2luigi.Task):
            def output(self):
                yield self.add_to_output("test.txt")

            @b2luigi.on_temporary_files
            def run(self):
                with open(self.get_output_file_name("test.txt"), "w") as f:
                    raise ValueError()
                    f.write("Test")

    Instead of creating the file "test.txt" at the beginning and filling it with content
    later (which will never happen because of the exception thrown, mich makes the file
    existing but the task actually not finished), the file will be written to a temporary
    file first and copied to its final location at the end of the run function (but only if there
    was no error).

    """
    @wraps(run_function)
    def run(self):
        with TemporaryFileContextManager(self):
            run_function(self)

    return run