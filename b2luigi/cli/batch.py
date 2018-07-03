import time

import luigi


def _get_status(task, task_list):
    try:
        return task_list[task.task_id]["status"]
    except KeyError:
        return "UNKNOWN"


def _schedulable(task, task_list):
    if _get_status(task, task_list) != "PENDING":
        return False

    for dep in task.deps():
        if _get_status(dep, task_list) != "DONE":
            return False

    return True


def _task_tree_iterator(tasks):
    for task in tasks:
        yield task
        yield from _task_tree_iterator(task.deps())


def check_for_possible_work(tasks, scheduler_host, scheduler_port):
    worker_scheduler_factory = luigi.interface._WorkerSchedulerFactory()
    sch = worker_scheduler_factory.create_remote_scheduler(url=f"http://{scheduler_host}:{scheduler_port}")

    worker = worker_scheduler_factory.create_worker(scheduler=sch, worker_processes=1, assistant=False)

    with worker:
        for t in tasks:
            worker.add(t, False, 0)

        while True:
            task_list = worker._scheduler.task_list()

            all_done = True
            schedulable_tasks = set()
            for task in _task_tree_iterator(tasks):
                if _schedulable(task, task_list):
                    schedulable_tasks.add(task)
                    all_done = False
                elif _get_status(task, task_list) == "RUNNING":
                    all_done = False

            if all_done:
                break

            if schedulable_tasks:
                # TODO: We could actually give out jobs to each of the workers!
                _submit_worker(len(schedulable_tasks), scheduler_host, scheduler_port)
            else:
                print("Nothing to do in the moment...")

            time.sleep(5)

    print(luigi.execution_summary.summary(worker))


def _submit_worker(num_workers, host, port):
    import subprocess
    import os
    import sys
    for _ in range(num_workers):
        subprocess.check_call(["bsub", "-env", "'all'", "python3", os.path.realpath(sys.argv[0]), "--scheduler-host", host,
                               "--scheduler-port", str(port), "--batch-runner"], env=os.environ.copy())
        print("Having send worker")