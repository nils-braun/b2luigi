import json

import luigi
import luigi.server
import luigi.configuration

from b2luigi.cli.batch import SendJobWorkerSchedulerFactory
from b2luigi.core.settings import set_setting
from b2luigi.core.utils import task_iterator


def run_as_batch_worker(task_list, cli_args, kwargs):
    for root_task in task_list:
        for task in task_iterator(root_task):
            if task.task_id != cli_args.task_id:
                continue

            # TODO: We do not process the information if (a) we have a new dependency and (b) why the task has failed.
            try:
                task.run()
                task.on_success()
                exit(0)
            except BaseException as ex:
                task.on_failure(ex)
                exit(1)


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
    set_setting("dispatch", False)
    luigi.build(task_list, log_level="DEBUG", local_scheduler=True, **kwargs)
