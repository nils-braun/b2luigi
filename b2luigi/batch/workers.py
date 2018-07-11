import enum

import luigi.interface
import luigi.worker

from b2luigi.batch.processes.lsf import LSFProcess
from b2luigi.batch.processes.test import TestProcess
from b2luigi.core.settings import get_setting


class BatchSystems(enum.Enum):
    lsf = "lsf"
    htcondor = "htcondor"
    local = "local"
    test = "test"


class SendJobWorker(luigi.worker.Worker):
    def _create_task_process(self, task):
        try:
            batch_system = task.batch_system
        except AttributeError:
            batch_system = BatchSystems(get_setting("batch_system", BatchSystems.lsf))

        if batch_system == BatchSystems.lsf:
            process_class = LSFProcess
        elif batch_system == BatchSystems.htcondor:
            raise NotImplementedError
        elif batch_system == BatchSystems.test:
            process_class = TestProcess
        elif batch_system == BatchSystems.local:
            return super()._create_task_process(task)
        else:
            raise NotImplementedError

        return process_class(task=task, scheduler=self._scheduler,
                             result_queue=self._task_result_queue, worker_timeout=self._config.timeout)


class SendJobWorkerSchedulerFactory(luigi.interface._WorkerSchedulerFactory):
    def create_worker(self, scheduler, worker_processes, assistant=False):
        return SendJobWorker(scheduler=scheduler, worker_processes=worker_processes, assistant=assistant)
