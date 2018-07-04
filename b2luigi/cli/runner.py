import luigi
import luigi.server
import luigi.configuration

from b2luigi.core.settings import set_setting


def run_local(task_list, cli_args, kwargs):
    if cli_args.batch:
        set_setting("batch", True)

    if cli_args.scheduler_host or cli_args.scheduler_port:
        core_settings = luigi.interface.core()
        host = cli_args.scheduler_host or core_settings.scheduler_host
        port = int(cli_args.scheduler_port) or core_settings.scheduler_port
        luigi.build(task_list, scheduler_host=host, scheduler_port=port, **kwargs)
    else:
        luigi.build(task_list, local_scheduler=True, **kwargs)


def run_test_mode(task_list, cli_args, kwargs):
    set_setting("dispatch", False)
    luigi.build(task_list, local_scheduler=True, log_level="INFO", **kwargs)
