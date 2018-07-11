from b2luigi.cli.arguments import get_cli_arguments
from b2luigi.cli import runner


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

    # Check the CLI arguments and run as requested
    cli_args = get_cli_arguments()

    show_output_flag = kwargs.pop("show_output", False)
    dry_run_flag = kwargs.pop("dry_run", False)
    test_flag = kwargs.pop("test", False)
    batch_flag = kwargs.pop("batch", False)

    if cli_args.show_output or show_output_flag:
        runner.show_all_outputs(task_list)
    elif cli_args.dry_run or dry_run_flag:
        runner.dry_run(task_list)
    elif cli_args.test or test_flag:
        runner.run_test_mode(task_list, cli_args, kwargs)
    elif cli_args.batch or batch_flag:
        runner.run_batched(task_list, cli_args, kwargs)
    elif cli_args.batch_runner:
        runner.run_as_batch_worker(task_list, cli_args, kwargs)
    else:
        runner.run_local(task_list, cli_args, kwargs)



