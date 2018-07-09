from b2luigi.cli.arguments import get_cli_arguments
from b2luigi.cli.runner import run_as_batch_worker, run_local, run_test_mode, run_batched, show_all_outputs, dry_run
from b2luigi.core import tasks

import os


__has_run_already = False


def process(task_like_elements, **kwargs):
    # Assert, that process is only run once
    global __has_run_already
    if __has_run_already:
        raise RuntimeError("You are not allowed to call process twice in your code!")
    __has_run_already = True

    # Create Task List
    if not isinstance(task_like_elements, list):
        task_list = [task_like_elements]
    else:
        task_list = task_like_elements

    # Run now if requested
    if os.environ.get("B2LUIGI_EXECUTION", False):
        return tasks.run_task_from_env()

    # Check the CLI arguments and run as requested
    cli_args = get_cli_arguments()

    if cli_args.show_output:
        show_all_outputs(task_list)
    elif cli_args.dry_run:
        dry_run(task_list)
    elif cli_args.test:
        run_test_mode(task_list, cli_args, kwargs)
    elif cli_args.batch:
        run_batched(task_list, cli_args, kwargs)
    elif cli_args.batch_runner:
        run_as_batch_worker(task_list, cli_args, kwargs)
    else:
        run_local(task_list, cli_args, kwargs)



