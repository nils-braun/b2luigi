import collections
import json

import luigi
import luigi.server
import luigi.configuration
import os

from b2luigi.batch.workers import SendJobWorkerSchedulerFactory
from b2luigi.core.settings import set_setting
from b2luigi.core.utils import task_iterator, get_all_output_files_in_tree


def run_as_batch_worker(task_list, cli_args, kwargs):
    for root_task in task_list:
        for task in task_iterator(root_task):
            if task.task_id != cli_args.task_id:
                continue

            set_setting("local_execution", True)

            # TODO: We do not process the information if (a) we have a new dependency and (b) why the task has failed.
            # TODO: Would be also nice to run the event handlers
            try:
                task.run()
                task.on_success()
            except BaseException as ex:
                task.on_failure(ex)
                raise ex


def run_batched(task_list, cli_args, kwargs):
    kwargs["worker_scheduler_factory"] = SendJobWorkerSchedulerFactory()
    run_local(task_list, cli_args, kwargs)


def run_local(task_list, cli_args, kwargs):
    if cli_args.scheduler_host or cli_args.scheduler_port:
        core_settings = luigi.interface.core()
        host = cli_args.scheduler_host or core_settings.scheduler_host
        port = int(cli_args.scheduler_port) or core_settings.scheduler_port
        luigi.build(task_list, log_level="INFO", scheduler_host=host, scheduler_port=port, **kwargs)
    else:
        luigi.build(task_list, log_level="INFO", local_scheduler=True, **kwargs)


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

        file_names = sorted(set(d["file_name"] for d in file_names))
        for file_name in file_names:
            # TODO: this is not correct as it does not check the task status!
            if os.path.exists(file_name):
                print("\t", Fore.GREEN, file_name, Style.RESET_ALL)
            else:
                print("\t", Fore.RED, file_name, Style.RESET_ALL)
        print()


def dry_run(task_list):
    for task in task_list:
        if not task.complete():
            exit(1)

    exit(0)
