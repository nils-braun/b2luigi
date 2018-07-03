import luigi
import luigi.server
import luigi.configuration

from b2luigi.cli.batch import check_for_possible_work
from b2luigi.core.settings import set_setting


def run_as_batch_worker(task_list, cli_args, kwargs):
    class OneTimeWorker(luigi.worker.Worker):
        def _run_task(self, task_id):
            super()._run_task(task_id)
            self._stop_requesting_work = True

    class OneTimeWorkerSchedulerFactory(luigi.interface._WorkerSchedulerFactory):
        def create_worker(self, scheduler, worker_processes, assistant=False):
            return OneTimeWorker(
                scheduler=scheduler, worker_processes=worker_processes, assistant=assistant)

    luigi.build(task_list, scheduler_host=cli_args.scheduler_host, scheduler_port=cli_args.scheduler_port,
                worker_scheduler_factory=OneTimeWorkerSchedulerFactory(), **kwargs)


def run_local(task_list, cli_args, kwargs):
    if cli_args.scheduler_host or cli_args.scheduler_port:
        core_settings = luigi.interface.core()
        host = cli_args.scheduler_host or core_settings.scheduler_host
        port = int(cli_args.scheduler_port) or core_settings.scheduler_port
        luigi.build(task_list, scheduler_host=host, scheduler_port=port, **kwargs)
    else:
        luigi.build(task_list, local_scheduler=True, **kwargs)


def run_test_mode(task_list, cli_args, kwargs):
    set_setting("dispatch", False)
    luigi.build(task_list, local_scheduler=True, **kwargs)


def run_batched(task_list, cli_args, kwargs):
    core_settings = luigi.interface.core()
    host = cli_args.scheduler_host or core_settings.scheduler_host
    port = int(cli_args.scheduler_port) or core_settings.scheduler_port

    check_for_possible_work(task_list, host, port)


