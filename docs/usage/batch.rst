.. _batch-label:

Submitting to the Batch System
==============================

We use the task definition file created in :ref:`quick-start-label` and submit it to the batch system.

The only thing you need to do for this is start your file with the option ``--batch``, e.g. like so

.. code-block:: bash

    python3 simple-task.py --batch

The output file and log files are written to the same folders and the same amount of work is done - the only
difference is that the calculation is now running on the batch system.

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

    python3 simple-task.py [--batch] --scheduler-host HOST --scheduler-port PORT

which works for batch as well as non-batch jobs.
You can now visit the url http://HOST:PORT with your browser and see a nice summary of the current progress
of your tasks.

You are now ready to face some more :ref:`advanced-label` or have a look into the :ref:`faq-label`.


Drawbacks of the batch mode
---------------------------

Although the batch mode has many benefits, it would be unfair to not mention its downsides:

*   You have to choose the queue depending in your requirements (e.g. wall clock time) by yourself. So you need to make
    sure that the tasks will actually finish before the batch system kills them because of timeout.
*   There is currently now resubmission implemented. This means dying jobs because of batch system failures are just
    dead. But because of the dependency checking mechanism of ``luigi`` it is simple to just redo the calculation
    and re-calculate what is missing.
*   The ``luigi`` feature to request new dependencies while task running (via yield) is not implemented for
    the batch mode.
*   We need to check the status of the tasks quite often. If your site has restrictions on this, you might fall into
    them.