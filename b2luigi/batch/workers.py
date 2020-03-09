import enum

import luigi.interface
import luigi.worker

from b2luigi.batch.processes.lsf import LSFProcess
from b2luigi.batch.processes.htcondor import HTCondorProcess
from b2luigi.batch.processes.gbasf2 import Gbasf2Process
from b2luigi.batch.processes.test import TestProcess
from b2luigi.core.settings import get_setting
from b2luigi.core.utils import create_output_dirs


class BatchSystems(enum.Enum):
    lsf = "lsf"
    htcondor = "htcondor"
    gbasf2 = "gbasf2"
    local = "local"
    test = "test"


class SendJobWorker(luigi.worker.Worker):
    def _create_task_process(self, task):
        batch_system = BatchSystems(get_setting("batch_system", default=BatchSystems.lsf, task=task))

        if batch_system == BatchSystems.lsf:
            process_class = LSFProcess
        elif batch_system == BatchSystems.htcondor:
            process_class = HTCondorProcess
        elif batch_system == BatchSystems.gbasf2:
            process_class = Gbasf2Process
        elif batch_system == BatchSystems.test:
            process_class = TestProcess
        elif batch_system == BatchSystems.local:
            create_output_dirs(task)
            return super()._create_task_process(task)
        else:
            raise NotImplementedError

        return process_class(task=task, scheduler=self._scheduler,
                             result_queue=self._task_result_queue, worker_timeout=self._config.timeout)


class SendJobWorkerSchedulerFactory(luigi.interface._WorkerSchedulerFactory):
    def create_worker(self, scheduler, worker_processes, assistant=False):
        return SendJobWorker(scheduler=scheduler, worker_processes=worker_processes, assistant=assistant)