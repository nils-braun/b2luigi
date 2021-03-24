.. _quick-start-label:

Quick Start
===========

We use a very simple task definition file and submit it to a LSF batch system.

.. hint::
    The default batch system currently is LSF, so if you do not change it, LSF will be
    used. Check out :ref:`batch-label` for more information.

Our task will be very simple: we want to create 100 files with some random number in it.
Later, we will build the average of those numbers.

1. Open a code editor and create a new file ``simple-example.py`` with the following content:

    .. literalinclude:: ../../tests/doc_examples/simple_example.py
       :linenos:

    Each building block in ``(b2)luigi`` is a :class:`b2luigi.Task`.
    It defines (which its run function), what should be done.
    A task can have parameters, as in our case the ``some_parameter`` defined in line 6.
    Each task needs to define, what it will output in its ``output`` function.

    .. note::

        We have defined a result path in the script with

        .. code-block:: python

            b2luigi.set_setting("results")

        You can ignore that for not - we will come back to it later.

    In our run function, we generate a random number and write it to the output file,
    which is named after the parameter of the task and stored in a result folder.

    .. hint::

        For those of you who have already used ``luigi`` most of this seems familiar.
        Actually, ``b2luigi``'s task is a superset of ``luigi``'s, so you can reuse
        your old scripts!
        ``b2luigi`` will not care, which one you are using.
        But we strongly advice you to use ``b2luigi``'s task, as it has some more
        superior functions (see below).

    Please not that we could have imported ``b2luigi`` with

    .. code-block:: python

        import b2luigi as luigi

    to make the transition between ``b2luigi`` and ``luigi`` even simpler.

2.  Call the newly created file with python:

    .. code-block:: bash

        python simple-example.py --batch

    Instead of giving the batch parameter in as argument, you can also add it
    to the ``luigi.process(.., batch=True)`` call.

    Each task will be scheduled as a batch job to your LSF queue.
    Using the dependency management of ``luigi``, the batch jobs are only scheduled when all dependencies are fulfilled
    saving you some unneeded CPU time on the batch system.
    This means although you have requested 200 workers, you only need
    100 workers to fulfill the tasks, so only 100 batch jobs will be started.
    On your local machine runs only the scheduling mechanism needing only a small amount of a single CPU power.

    .. hint::

        If you have no LSF queue ready or you do not want to run on the batch,
        you can also remove the `batch` argument.
        This will fall back to a normal ``luigi`` execution.
        Please see :ref:`batch-label` for more information on batch execution
        and the discussion of other batch systems.


3.  After the job is completed, you will see something like:

    .. code-block::

        ===== Luigi Execution Summary =====

        Scheduled 100 tasks of which:
        * 100 ran successfully:
            - 100 MyTask(some_parameter=0,1,10,11,12,13,14,15,16,17,18,...)

        This progress looks :) because there were no failed tasks or missing dependencies

        ===== Luigi Execution Summary =====

    The log files for each task are written to the ``logs`` folder.

    After a job is submitted, ``b2luigi`` will check if it is still running or not and handle failed or done tasks correctly.

4.  The defined output file names will in most of the cases depend on the parameters of the task, as
    you do not want to override your files from different tasks.
    However this means, you always need to include all parameters in the file name to keep them different.
    This cumbersome work can be handled by ``b2luigi`` automatically ,
    which will also help you ordering your files at no cost.
    This is especially useful in larger projects, when many people are defining and executing tasks.

    This code listing shows the same task, but this time written using the helper
    functions given by ``b2luigi``.

    .. literalinclude:: ../../tests/doc_examples/simple_example_b2luigi.py
       :linenos:

    Before continuing, remove the output of the former calculation.

    .. code-block:: bash

        rm -rf results

    If you now call

    .. code-block:: bash

        python simple-example.py --batch

    you are basically doing the same as before, with some very nice benefits:

        * The parameter values are automatically added to the output file (have a look into the ``results/``
          folder to see how it works and where the results are stored)
        * The output for different parameters are stored on different locations, so no need to fear overriding
          results.
        * The format of the folder structure makes it easy to work on it using bash commands as well as
          automated procedures.
        * Other files related to your job, e.g. the submission files etc. are also placed into this
          folder (this is why the very first example defined it already).
        * The default is to use the folder where your script is located.

    .. hint::
        In the example, the base path for the results is defined in the python file with

        .. code-block:: python

            b2luigi.set_setting("result_dir", "results")

        Instead, you can also add a ``settings.json`` with the following content
        in the folder where your script lives:

        .. literalinclude:: ../../tests/doc_examples/settings.json
            :language: json

        The ``settings.json`` will be used by all tasks in this folder and in each sub-folder.
        You can use it to define project settings (like result folders) and specific settings for your
        local sub project. Read the documentation on :meth:`b2luigi.get_setting` for
        more information on how to use it.

    .. attention::
        The result path (as well as any other paths, e.g. the log folders) are always evaluated
        relatively to your script file.
        This means ``results`` will always be created in the folder where your script is,
        not where your current working directory is.
        If you are unsure on the location, call

        .. code-block:: bash

            python3 simple-example.py --show-output

        More on file systems is described in :ref:`batch-label`, which is also mostly
        true for non-batch calculations.

5.  Let's add some more tasks to our little example. We want to use the currently created files
    and add them all together to an average number.
    So edit your example file to include the following content:

    .. literalinclude:: ../../tests/doc_examples/simple_example_b2luigi_2.py
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

You are now ready to read some more documentation in :ref:`api-documentation-label` or have a look into the :ref:`faq-label`.
Please also check out the different :ref:`run-modes-label`.
