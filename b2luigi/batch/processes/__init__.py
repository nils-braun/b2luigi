import os
import sys
import time

import luigi


class BatchProcess:
    def __init__(self, result_queue, worker_timeout, task, scheduler):
        self.use_multiprocessing = False
        self.exitcode = 0
        self.task = task
        self.timeout_time = time.time() + worker_timeout if worker_timeout else None

        self._result_queue = result_queue
        self._scheduler = scheduler

        self.task_cmd = [sys.executable, os.path.realpath(sys.argv[0]),
                         "--batch-runner",
                         "--task-id", task.task_id]

    def run(self):
        raise NotImplementedError

    def is_alive(self):
        raise NotImplementedError

    def terminate(self):
        raise NotImplementedError

    def put_to_result_queue(self):
        expl = ""
        missing = []
        new_deps = []
        self._result_queue.put((self.task.task_id, luigi.scheduler.DONE, expl, missing, new_deps))