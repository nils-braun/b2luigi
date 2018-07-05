.. _batch-label:

Submitting to the Batch System
==============================

We use the task definition file created in :ref:`quick-start-label` and submit it to the batch system.

You need to do two things for this: setup a central scheduler that keeps track of your work and start the tasks with
the batch mode.

Start a Central Scheduler
-------------------------

The only thing you have to do is call the ``luigid`` executable. Where to find this depends on your basf2 installation type:

a. If you are using a central release from cvmfs, luigid is installed into your home directory:

.. code-block:: bash

    ~/.local/bin/luigid --port PORT

b. If you have a local installation, you can just call the executable as it is already in your path:

.. code-block:: bash

    luigid --port PORT

The default port is 8082, but you can choose any non-occupied port.

The central scheduler will register the tasks you want to process and keep track of which tasks are already done.


Start the tasks in batch mode
-----------------------------

Starting the tasks in batch mode works basically the same as described in :ref:`quick-start-label`.
You only need to supply the information on the started central scheduler and that you want to run on the batch system:

.. code-block:: bash

    basf2 simple-task.py -- --batch --scheduler-port PORT --scheduler-host HOST

where PORT is the port you have chosen before for the central scheduler and HOST is the name of the host you are running
it (e.g. cw07)

This will schedule a job for each task whenever it is ready to run (when all dependencies are fulfilled).
The output for you is basically the same and also the output file are written to the same folders - the only
difference is that the calculation is now running on the batch system.

You are now ready to face some more :ref:`advanced-examples-label` or have a look into the :ref:`faq-label`.
