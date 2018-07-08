import enum

import luigi.interface

from b2luigi.batch.processes.lsf import LSFProcess
from b2luigi.core.settings import get_setting


class BatchSystems(enum.Enum):
    lsf = "lsf"
    htcondor = "htcondor"
    local = "local"


class SendJobWorker(luigi.worker.Worker):
    def _create_task_process(self, task):
        try:
            batch_system = task.batch_system
        except AttributeError:
            batch_system = get_setting("batch_system", BatchSystems.lsf)

        if batch_system == BatchSystems.lsf:
            return LSFProcess(task=task, scheduler=self._scheduler,
                              result_queue=self._task_result_queue, worker_timeout=self._config.timeout)
        elif batch_system == BatchSystems.htcondor:
            raise NotImplementedError
        elif batch_system == BatchSystems.local:
            return super()._create_task_process(task)
        else:
            raise NotImplementedError


class SendJobWorkerSchedulerFactory(luigi.interface._WorkerSchedulerFactory):
    def create_worker(self, scheduler, worker_processes, assistant=False):
        return SendJobWorker(scheduler=scheduler, worker_processes=worker_processes, assistant=assistant)