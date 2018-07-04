import luigi
import luigi.interface
import six

import subprocess


class SendJobWorker(luigi.worker.Worker):
    def _purge_children(self):
        # Never purge children!
        return

    def _run_task(self, task_id):
        if task_id in self._running_tasks:
            next(self._sleeper())
            return

        # TODO: Depend on job whether to send this to the queue or not
        task = self._scheduled_tasks[task_id]

        self._running_tasks[task_id] = object()

        with open("log", "a") as f:
            f.write(task_id + "\n")

        print("Scheduling new job", task_id)
        # TODO: correct schedule
        subprocess.Popen(["bsub", "-env all", "python3", "example.py", "--batch-runner",
                          "--scheduler-url", self._scheduler._url,
                          "--worker-id", self._id, "--task-id", task_id],
                         #stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                         )

    def run(self):
        # Basically a copy from the parents run function
        sleeper = self._sleeper()
        self.run_succeeded = True

        self._add_worker()

        while True:
            # Update out own list of "running" tasks
            for key, task in self._scheduler.task_list().items():
                if task["status"] not in ["PENDING", "RUNNING"] and key in self._running_tasks:
                    del self._running_tasks[key]
                    sched_task = self._scheduled_tasks.get(key)
                    self._add_task_history.append((sched_task, task["status"], True))

            get_work_response = self._get_work()

            if get_work_response.worker_state == luigi.worker.WORKER_STATE_DISABLED:
                self._start_phasing_out()

            if get_work_response.task_id is None:
                if not self._stop_requesting_work:
                    self._log_remote_tasks(get_work_response)
                if len(self._running_tasks) == 0:
                    if self._keep_alive(get_work_response):
                        six.next(sleeper)
                        continue
                    else:
                        break
                else:
                    self._handle_next_task()
                    continue

            # task_id is not None:
            self._run_task(get_work_response.task_id)

        return self.run_succeeded


class SendJobWorkerSchedulerFactory(luigi.interface._WorkerSchedulerFactory):
    def create_worker(self, scheduler, worker_processes, assistant=False):
        return SendJobWorker(scheduler=scheduler, assistant=assistant)


class OneTimeRemoteScheduler(luigi.rpc.RemoteScheduler):
    def add_task(self, **kwargs):
        # We do not want to send a pending message to the central scheduler, as this would override already given
        # information by other workers
        if kwargs["status"] == "PENDING":
            return
        super().add_task(**kwargs)


class OneTimeWorker(luigi.worker.Worker):
    def __init__(self, task_id, **kwargs):
        super().__init__(**kwargs)
        self._task_id = task_id

    def run(self):
        # The run is quite simple, as we only want to do a single task
        self._add_worker()
        self._run_task(self._task_id)
        self._handle_next_task()

        return True


class OneTimeWorkerSchedulerFactory(luigi.interface._WorkerSchedulerFactory):
    def __init__(self, task_id, worker_id):
        self.task_id = task_id
        self.worker_id = worker_id

        super().__init__()

    def create_remote_scheduler(self, url):
        return OneTimeRemoteScheduler(url)

    def create_worker(self, scheduler, worker_processes, assistant=False):
        return OneTimeWorker(worker_id=self.worker_id, task_id=self.task_id,
                             scheduler=scheduler, worker_processes=worker_processes, assistant=assistant)
