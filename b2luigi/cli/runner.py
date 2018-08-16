import collections

import luigi
import luigi.server
import luigi.configuration
import os

from b2luigi.batch.workers import SendJobWorkerSchedulerFactory
from b2luigi.core.settings import set_setting
from b2luigi.core.utils import task_iterator, get_all_output_files_in_tree


def run_as_batch_worker(task_list, cli_args, kwargs):
    found_task = False
    for root_task in task_list:
        for task in task_iterator(root_task):
            if task.task_id != cli_args.task_id:
                continue

            found_task = True
            set_setting("local_execution", True)

            # TODO: We do not process the information if (a) we have a new dependency and (b) why the task has failed.
            # TODO: Would be also nice to run the event handlers
            try:
                task.run()
                task.on_success()
            except BaseException as ex:
                task.on_failure(ex)
                raise ex

            return

    if not found_task:
        raise ValueError(f"The task id {task.task_id} to be executed by this batch worker "
                         f"does not exist in the locally reproduced task graph.")


def run_batched(task_list, cli_args, kwargs):
    kwargs["worker_scheduler_factory"] = SendJobWorkerSchedulerFactory()
    run_local(task_list, cli_args, kwargs)


def run_local(task_list, cli_args, kwargs):
    if cli_args.scheduler_host or cli_args.scheduler_port:
        core_settings = luigi.interface.core()
        host = cli_args.scheduler_host or core_settings.scheduler_host
        port = int(cli_args.scheduler_port) or core_settings.scheduler_port
        kwargs["scheduler_host"] = host
        kwargs["scheduler_port"] = port
    else:
        kwargs["local_scheduler"] = True

    kwargs.setdefault("log_level", "INFO")
    luigi.build(task_list, **kwargs)


def run_test_mode(task_list, cli_args, kwargs):
    set_setting("local_execution", True)
    luigi.build(task_list, log_level="DEBUG", local_scheduler=True, **kwargs)


def show_all_outputs(task_list, *args, **kwargs):
    from colorama import Fore, Style

    all_output_files = collections.defaultdict(list)

    for task in task_list:
        output_files = get_all_output_files_in_tree(task)
        for key, file_names in output_files.items():
            all_output_files[key] += file_names

    for key, file_names in all_output_files.items():
        print(key)

        file_names = {d["file_name"]: d["exists"] for d in file_names}
        for file_name, exists in file_names.items():
            # TODO: this is not correct as it does not check the task status!
            if exists:
                print("\t", Fore.GREEN, file_name, Style.RESET_ALL)
            else:
                print("\t", Fore.RED, file_name, Style.RESET_ALL)
        print()


def dry_run(task_list):
    nonfinished_task_list = collections.defaultdict(set)

    for root_task in task_list:
        for task in task_iterator(root_task, only_non_complete=True):
            nonfinished_task_list[task.__class__.__name__].add(task)

    non_completed_tasks = 0
    for task_class in sorted(nonfinished_task_list):
        print(task_class)
        for task in nonfinished_task_list[task_class]:
            print("\tWould run", task)
            print()

            non_completed_tasks += 1

    if non_completed_tasks:
        print("In total", non_completed_tasks)
        exit(1)
    else:
        print("All tasks are finished!")
        exit(0)
