import contextlib
import importlib.util
import os

import argparse

from collections import defaultdict


@contextlib.contextmanager
def remember_cwd():
    old_cwd = os.getcwd()
    try:
        yield
    finally:
        os.chdir(old_cwd)


def get_all_outputs(task, only_tasks=False):
    if only_tasks:
        yield task.complete(), task
    else:
        outputs = task.output()
        if isinstance(outputs, dict):
            outputs = outputs.values()
        for output in outputs:
            yield output.exists(), output.path

    requirements = task.requires()

    try:
        for requirement in requirements:
            yield from get_all_outputs(requirement, only_tasks=only_tasks)
    except TypeError:
        yield from get_all_outputs(requirements, only_tasks=only_tasks)
    


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("file_name", help="Which file name to check.")
    parser.add_argument("--task_name", help="Which task name to check", default="MainTask")
    parser.add_argument("--only-tasks", help="Check only tasks", action="store_true")
    parser.add_argument("--only-missing", help="Show only missing entities", action="store_true")

    args = parser.parse_args()

    file_name = args.file_name
    task_class = args.task_name
    only_tasks = args.only_tasks
    only_missing = args.only_missing
    kwargs = {}

    file_name = os.path.abspath(file_name)

    with remember_cwd():
        os.chdir(os.path.dirname(file_name))
        spec = importlib.util.spec_from_file_location("module.name", file_name)
        task_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(task_module)

        m = getattr(task_module, task_class)(**kwargs)

        output_files = defaultdict(set)
        for key, value in get_all_outputs(m, only_tasks=only_tasks):
            output_files[key].add(value)

        for key, file_list in output_files.items():
            if key and only_missing:
                 continue
            try:
                file_list = sorted(file_list)
            except TypeError:
                pass

            if key:
                string = "Existing"
            else:
                string = "Missing"
            if file_list:
                print(f"{string} targets:")
                for output_file in file_list:
                    print(output_file)
            else:
                print(f"No {string.lower()} targets!")

            print()
