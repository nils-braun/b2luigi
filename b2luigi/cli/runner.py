import json

import luigi
import luigi.server
import luigi.configuration
from luigi.worker import _get_retry_policy_dict

from b2luigi.cli.batch import SendJobWorkerSchedulerFactory
from b2luigi.core.settings import set_setting
from b2luigi.core.utils import task_iterator


def run_as_batch_worker(task_list, cli_args, kwargs):
    scheduler = luigi.rpc.RemoteScheduler(cli_args.scheduler_url)

    for root_task in task_list:
        for task in task_iterator(root_task):
            if task.task_id == cli_args.task_id:
                try:
                    task.run()
                    status = "DONE"
                    expl = task.on_success()
                except BaseException as ex:
                    status = "FAILED"
                    expl = task.task.on_failure(ex)

                # TODO: Use a TaskProcess here?

                scheduler.add_task(worker=cli_args.worker_id,
                                   task_id=cli_args.task_id,
                                   status=status,
                                   expl=json.dumps(expl),
                                   resources=task.process_resources(),
                                   runnable=None,
                                   params=task.to_str_params(),
                                   family=task.task_family,
                                   module=task.task_module,
                                   new_deps=[], # TODO
                                   assistant=False,
                                   retry_policy_dict=_get_retry_policy_dict(task))


def run_batched(task_list, cli_args, kwargs):
    luigi.build(task_list, scheduler_host=cli_args.scheduler_host, scheduler_port=cli_args.scheduler_port,
                worker_scheduler_factory=SendJobWorkerSchedulerFactory(),
                log_level="INFO", **kwargs)


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
