from b2luigi.cli.arguments import get_cli_arguments
from b2luigi.cli import runner


__has_run_already = False


def process(task_like_elements, show_output=False, dry_run=False, test=False, batch=False, **kwargs):
    """
    Call this function in your main method to tell ``b2luigi`` where your entry
    point of the task graph is.
    It is very similar to ``luigi.build`` with some additional configuration options.

    Example:
        This example defines a simple task and tells ``b2luigi`` to execute it 100 times
        with different parametes::

            import b2luigi
            import random


            class MyNumberTask(b2luigi.Task):
                some_parameter = b2luigi.Parameter()

                def output(self):
                    return b2luigi.LocalTarget(f"results/output_file_{self.some_parameter}.txt")

                def run(self):
                    random_number = random.random()
                    with self.output().open("w") as f:
                        f.write(f"{random_number}\\n")

            if __name__ == "__main__":
                b2luigi.process([MyNumberTask(some_parameter=i) for i in range(100)])

    All flag arguments can also be given as command line arguments.
    This means the call with::

        b2luigi.process(tasks, batch=True)
    
    is equivalent to calling the script with::

        python script.py --batch

    Args:
        task_like_elements (:obj:`Task` or list): Task(s) to execute with luigi. 
            Can either be a list of tasks or a task instance.

        show_output (bool, optional): Instead of running the task(s), write out all output files
            which will be generated marked in color, if they are present already.
            Good for testing of your tasks will do, what you think they should.

        dry_run (bool, optional): Instead od running the task(s), write out which tasks will
            be executed. This is a simplified form of dependency resolution, so this 
            information may be wrong in some corner cases. Also good for testing.

        test (bool, optional): Does neither run on the batch system, with multiprocessing
            or dispatched (see :obj:`DispatchableTask`) but directly on the machine for 
            debugging reasons. Does output all logs to the console.

        batch (bool, optional): Execute the tasks on the selected batch system.
            Refer to :ref:`quick-start-label` for more information.
            The default batch system is LSF, but this can be changed with the `batch_system`
            settings. See :obj:`get_setting` on how to define settings.

        **kwargs: Additional keyword arguments passed to ``luigi.build``.

    Warning:
        You should always have just a single call to ``process`` in your script.
        If you need to have multiple calls, either use a :class:`b2luigi.WrapperTask`
        or two scripts.

    """
    # Assert, that process is only run once
    global __has_run_already
    if __has_run_already:
        raise RuntimeError(
            "You are not allowed to call process twice in your code!")
    __has_run_already = True

    # Create Task List
    if not isinstance(task_like_elements, list):
        task_list = [task_like_elements]
    else:
        task_list = task_like_elements

    # Check the CLI arguments and run as requested
    cli_args = get_cli_arguments()

    if cli_args.show_output or show_output:
        runner.show_all_outputs(task_list)
    elif cli_args.dry_run or dry_run:
        runner.dry_run(task_list)
    elif cli_args.test or test:
        runner.run_test_mode(task_list, cli_args, kwargs)
    elif cli_args.batch_runner:
        runner.run_as_batch_worker(task_list, cli_args, kwargs)
    elif cli_args.batch or batch:
        runner.run_batched(task_list, cli_args, kwargs)
    else:
        runner.run_local(task_list, cli_args, kwargs)
