.. _quick-start-label:

Quick Start
===========

We use a very simple task definition file and submit it to a LSF batch system.

.. hint::

    Currently, there is only an implementation for the LSF batch system. More will come soon!

Our task will be very simple: we want to create 100 files with some random number in it.
Later, we will build the average of those numbers.

1. Open a code editor and create a new file ``simple-example.py`` with the following content:

    .. literalinclude:: ../../tests/doc_examples/test_simple_example.py
       :linenos:

    Each building block in ``(b2)luigi`` is a :class:`b2luigi.Task`.
    It defines (which its run function), what should be done.
    A task can have parameters, as in our case the ``some_parameter`` defined in line 6.
    Each task needs to define, what it will output in its ``output`` function.

    In our run function, we generate a random number and write it to the output file,
    which is named after the parameter of the task and stored in a result folder.

    .. hint::

        For those of you who have already used ``luigi`` most of this seems familiar.
        Actually, ``b2luigi``'s task is a superset of ``luigi``'s, so you can reuse
        your old scripts!
        ``b2luigi`` will not care, which one you are using.
        But we strongly advice you to use ``b2luigi``'s task, as it has some more
        superior functions (see below).

2.  Call the newly created file with python:

    .. code-block:: bash

        python simple-example.py --batch

    Instead of giving the batch parameter in as argument, you can also add it
    to the ``b2luigi.process(.., batch=True)`` call.

    Each task will be scheduled as a batch job to your LSF queue.
    Using the dependency management of ``luigi``, the batch jobs are only scheduled when all dependencies are fulfilled
    saving you some unneeded CPU time on the batch system.
    This means although you have requested 200 workers, you only need
    100 workers to fulfill the tasks, so only 100 batch jobs will be started.
    On your local machine runs only the scheduling mechanism needing only a small amount of CPUs.

    .. hint::

        If you have no LSF queue ready, you can also remove the `batch` argument.
        This will fall back to a normal ``luigi`` execution.

3.  After the job is completed, you will see something like:

    .. code-block::

        ===== Luigi Execution Summary =====

        Scheduled 100 tasks of which:
        * 100 ran successfully:
            - 100 MyTask(some_parameter=0,1,10,11,12,13,14,15,16,17,18,...)

        This progress looks :) because there were no failed tasks or missing dependencies

        ===== Luigi Execution Summary =====

    The log files for each task are written to the `logs` folder.

    After a job is submitted, ``b2luigi`` will check if it is still running or not and handle failed or done tasks correctly.

4.  The defined outputs will in most of the cases depend on the parameters of the task, as
    you do not want to override your files from different tasks.
    The cumbersome work of keeping track of the correct outputs can be handled by ``b2luigi``,
    which will also help you ordering your files at no cost.
    This is especially useful in larger projects, when many people are defining and executing tasks.

    This code listing shows the same task, but this time written using the helper
    functions given by ``b2luigi``.

    .. literalinclude:: ../../tests/doc_examples/test_simple_example_b2luigi.py
       :linenos:

    Before you execute the file (e.g. with ``--batch``), add a ``settings.json`` with the following content
    in your current working directory:

    .. literalinclude:: ../../tests/doc_examples/settings.json
       :language: json

    If you now call

    .. code-block:: bash

        python simple-example.py --batch

    you are basically doing the same as before, with some very nice benefits:

        * The parameter values are automatically added to the output file (have a look into the `results/`
          folder to see how it works)
        * The ``settings.json`` will be used by all tasks in this folder and in each sub-folder.
          You can use it to define project settings (like result folders) and specific settings for your
          local sub project. Read the documentation on :meth:`b2luigi.get_setting` for
          more information on how to use it.

5.  Let's add some more tasks to our little example. We want to use the currently created files
    and add them all together to an average number.
    So edit your example file to include the following content:

    .. literalinclude:: ../../tests/doc_examples/test_simple_example_b2luigi_2.py
       :linenos:

    See how we defined dependencies in line 19 with the ``requires`` function.
    By calling ``clone`` we make sure that any parameters from the current task (which are none in our case)
    are copied to the dependencies.

    .. hint::

        Again, expert ``luigi`` users will not see anything new here.

    By using the helper functions :meth:`b2luigi.Task.get_input_file_names`
    and :meth:`b2luigi.Task.get_output_file` the output file name generation with parameters
    is transparent to you as a user.
    Super easy!

    When you run the script, you will see that ``luigi`` detects your already run files
    from before (the random numbers) and will not run the task again!
    It will only output a file in `results/average.txt` with a number near 0.5.

You are now ready to face some more :ref:`advanced-label` or have a look into the :ref:`faq-label`.

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
