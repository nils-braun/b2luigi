from contextlib import ExitStack


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


def on_temporary_files(f):
    def run(self):
        with TemporaryFileContextManager(self):
            f(self)

    return run