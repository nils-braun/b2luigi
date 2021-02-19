.. _run-modes-label:

Run Modes
=========

The run mode can be chosen by calling your python file with

.. code-block:: bash

    python file.py --mode

or by calling ``b2luigi.process`` with a given mode set to ``True``

.. code-block:: python

    b2luigi.process(.., mode=True)

where mode can be one of:

*   **batch**: Run the tasks on a batch system, as described in :ref:`quick-start-label`. The maximal number of
    batch jobs to run in parallel (jobs in flight) is equal to the number of workers.
    This is 1 by default, so you probably want to change this.
    By default, LSF is used as a batch system. If you want to change this, set the corresponding ``batch_system``
    (see :ref:`batch-label`) to one of the supported systems.

*   **dry-run**: Similar to the dry-run funtionality of ``luigi``, this will not start any tasks but just tell
    you, which tasks it would run, and call the ``dry_run`` method of the task if implemented:

    .. code-block:: python

        class SomeTask(b2luigi.Task):
            [...]
            def dry_run(self):
                # if a method with this name is provided, it will be executed
                # automatically when starting the processing in dry-run mode
                do_some_stuff_in_dry_run_mode()

    This feature can be easily used for e.g. file name debugging, i.e. to print out the file names ``b2luigi`` 
    will create when running the actual task. The exit code of the ``dry-run`` mode is 1 in case a task needs 
    to run and 0 otherwise.

*   **show-output**: List all output files that this has produced/will produce. Files which already exist
    (where the targets define, what exists mean in this case) are marked as green whereas missing targets are
    marked red.

*   **test**: Run the tasks normally (no batch submission), but turn on debug logging of ``luigi``. Also,
    do not dispatch any task (if requested) and print the output to the console instead of in log files.

Additional console arguments:

*   **--scheduler-host** and **--scheduler-port**: If you have set up a central scheduler, you can pass this information
    here easily. This works for batch or non-batch submission but is turned of for the test mode.


Start a Central Scheduler
-------------------------

When the number of tasks grows, it is sometimes hard to keep track of all of them (despite the summary in the end).
For this, ``luigi`` (the parent project of ``b2luigi``) brings a nice visualisation and scheduling tool called the central scheduler.

To start this you need to call the ``luigid`` executable.
Where to find this depends on your installation type:

a. If you have a installed ``b2luigi`` without user flag, you can just call the executable as it is already in your path:

    .. code-block:: bash

        luigid --port PORT

b. If you have a local installation, luigid is installed into your home directory:

    .. code-block:: bash

        ~/.local/bin/luigid --port PORT

The default port is 8082, but you can choose any non-occupied port.

The central scheduler will register the tasks you want to process and keep track of which tasks are already done.

To use this scheduler, call ``b2luigi`` by giving the connection details:

.. code-block:: bash

    python simple-task.py [--batch] --scheduler-host HOST --scheduler-port PORT

which works for batch as well as non-batch jobs.
You can now visit the url http://HOST:PORT with your browser and see a nice summary of the current progress
of your tasks.
