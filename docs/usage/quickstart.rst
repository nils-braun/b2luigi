.. _quick-start-label:

Quick Start
===========

We use a very simple task definition file and submit it to a LSF batch system.

.. attention::

    Currently, there is only an implementation for the LSF batch system. More will come soon!

1. Open a code editor and create a new file ``simple-example.py`` with the following content:

    .. code-block:: python

        import b2luigi


        class MyTask(b2luigi.Task):
            some_parameter = b2luigi.Parameter()

            def output(self):
                return b2luigi.LocalTarget(f"output_file_{self.some_parameter}.txt")

            def run(self):
                with self.output().open("w") as f:
                    f.write("This is a test\n")


        if __name__ == "__main__":
            b2luigi.process([MyTask(some_parameter=i) for i in range(100)], workers=200, batch=True)

2. Call the newly created file with python:

    .. code-block:: bash

        python simple-example.py

    Instead of giving the batch parameter in the file ``b2luigi.process(.., batch=True)`` you can also call the file
    with the ``--batch`` parameter

    .. code-block:: bash

        python simple-example.py --batch

    Instead of running locally, the jobs will be scheduled to your LSF queue.
    Although you have requested 200 workers, there will only be a single process on your machine and as you only need
    100 workers to fulfill the tasks, also only 100 batch jobs will be started.

3. After the job is completed, you will see something like:

    .. code-block::

        ===== Luigi Execution Summary =====

        Scheduled 100 tasks of which:
        * 100 ran successfully:
            - 100 MyTask(some_parameter=0,1,10,11,12,13,14,15,16,17,18,...)

        This progress looks :) because there were no failed tasks or missing dependencies

        ===== Luigi Execution Summary =====

The output file are written as if you would have called the task from your current working directory.
The log files for each task are written to the same folder.

``b2luigi`` will schedule a single batch job for each requested task.
Using the dependency management of ``luigi``, the batch jobs are only scheduled when all dependencies are fulfilled
saving you some unneeded CPU time on the batch system.
After a job is submitted, ``b2luigi`` will check if it is still running or not and handle failed or done tasks correctly.


Choosing the LSF queue
----------------------

By default, all tasks will be sent to the short queue. This behaviour can be changed on a per task level by giving
the task a property called ``queue`` and setting it to the queue it should run on, e.g.

.. code-block:: python

    class MyLongTask(b2luigi.Task):
        queue = "l"



Start a Central Scheduler
-------------------------

When the number of tasks grows, it is sometimes hard to keep track of all of them (despite the summary in the end).
For this, ``luigi`` brings a nice visualisation tool called the central scheduler.

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

You are now ready to face some more :ref:`advanced-label` or have a look into the :ref:`faq-label`.


Drawbacks of the batch mode
---------------------------

Although the batch mode has many benefits, it would be unfair to not mention its downsides:

*   We are currently assuming that you have the same environment setup on the batch system as locally
    (actually, we are copying the console environment variables) and we will call the python executable which runs
    your scheduling job.
*   You have to choose the queue depending in your requirements (e.g. wall clock time) by yourself. So you need to make
    sure that the tasks will actually finish before the batch system kills them because of timeout.
*   There is currently now resubmission implemented. This means dying jobs because of batch system failures are just
    dead. But because of the dependency checking mechanism of ``luigi`` it is simple to just redo the calculation
    and re-calculate what is missing.
*   The ``luigi`` feature to request new dependencies while task running (via yield) is not implemented for
    the batch mode.
