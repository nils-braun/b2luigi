.. _batch-label:

Batch Processing
================

As shown in :ref:`quick-start-label`, using the batch instead of local processing is really just a ``--batch``
on the command line or calling ``process`` with ``batch=True``.
However, there is more to discover!

Choosing the batch system
-------------------------

Using ``b2luigi``'s settings mechanism (described here :meth:`b2luigi.get_setting`) you can choose which
batch system should be used.
Currently, ``htcondor`` and ``lsf`` are supported.
There is also an experimental wrapper for ``gbasf2``, the Belle II
submission tool for the LHC Worlwide Computing Grid, which works for ``Basf2PathTask`` tasks.
More will come soon (PR welcome!).


Choosing the Environment
------------------------

If you are doing a local calculation, all calculated tasks will use the same environment (e.g. ``$PATH`` setting, libraries etc.)
as you have currently set up when calling your script(s).
This makes it predictable and simple.

Things get a bit more complicated when using a batch farm, as the workers might not have the same environment set up, the batch
submission does not copy the environment (or the local site administrators have forbidden that) or the system on the workers
is so different that copying the environment from the scheduling machine does not make sense.

Therefore ``b2luigi`` provides you with three mechanism to set the environment for each task:

* You can give a bash script in the ``env_script`` setting (via ``set_setting()``, ``settings.json`` or for each task as usual,
  see :meth:`b2luigi.get_setting`), which will be called even before anything else on the worker.
  Use it to set up things like the path variables or the libraries (e.g. when you are using a virtual environment) and your
  batch system does not support environment copy from the scheduler to the workers.
  For example a useful script might look like this:

  .. code-block:: bash

    # Source my virtual environment
    source venv/bin/activate
    # Set some specific settings
    export MY_IMPORTANT_SETTING 10

* You can set the ``env`` setting to a dictionary, which contains additional variables to be set up before your job runs.
  Using the mechanism described in :meth:`b2luigi.get_setting` it is possible to make this task- or even parameter-dependent.

* By default, ``b2luigi`` re-uses the same ``python`` executable on the workers as you used to schedule the tasks (by calling your script).
  In some cases, this specific python executable is not present on the worker or is not usable (e.g. because of different operation systems
  or architectures).
  You can choose a new executable with the ``executable`` setting (it is also possible to just use ``python3`` as the executable assuming
  it is in the path).
  The executable needs to be callable after your ``env_script`` or your specific ``env`` settings are used.
  Please note, that the ``environment`` setting is a list, so you need to pass your python executable with possible arguments like this:

  .. code-block:: python

    b2luigi.set_setting("executable", ["python3"])


File System
-----------

Depending on your batch system, the filesystem on the worker processing the task and the scheduler machine can be different or even unrelated.
Different batch systems and batch systems implementations treat this fact differently.
In the following, the basic procedure and assumption is explained.
Any deviation from this is described in the next section.

By default, ``b2luigi`` needs at least two folders to be accessible from the scheduling as well as worker machine:
the result folder and the folder of your script(s).
If possible, use absolute paths for the result and log directory to prevent any problems.
Some batch systems (e.g. htcondor) support file copy mechanisms from the scheduler to the worker systems.
Please checkout the specifics below.

.. hint::

    All relative paths given to e.g. the ``result_dir`` or the ``log_dir`` are always evaluated
    relative to the folder where your script lives.
    To prevent any disambiguities, try to use absolute paths whenever possible.

Some batch system starts the job in an arbitrary folder on the workers instead of the current folder on the scheduler.
That is why ``b2luigi`` will change the directory into the path of your called script before starting the job.

In case your script is accessible from a different location on the worker than on the scheduling machine, you can give the setting ``working_dir``
to specify where the job should run.
Your script needs to be in this folder and every relative path (e.g. for results or log) will be evaluated from there.

Drawbacks of the batch mode
---------------------------

Although the batch mode has many benefits, it would be unfair to not mention its downsides:

*   You have to choose the queue/batch settings/etc. depending in your requirements (e.g. wall clock time) by yourself.
    So you need to make sure that the tasks will actually finish before the batch system kills them because of timeout.
    There is just no way for ``b2luigi`` to know this beforehand.
*   There is currently no resubmission implemented.
    This means dying jobs because of batch system failures are just dead.
    But because of the dependency checking mechanism of ``luigi`` it is simple to just redo the calculation
    and re-calculate what is missing.
*   The ``luigi`` feature to request new dependencies while task running (via ``yield``) is not implemented for
    the batch mode so far.


Batch System Specific Settings
------------------------------

Every batch system has special settings.
You can look them up here:

LSF
...

.. autoclass:: b2luigi.batch.processes.lsf.LSFProcess
    :show-inheritance:

HTCondor
........

.. autoclass:: b2luigi.batch.processes.htcondor.HTCondorProcess
    :show-inheritance:

GBasf2 Wrapper for LCG
......................

.. autoclass:: b2luigi.batch.processes.gbasf2.Gbasf2Process
    :show-inheritance:

Add your own batch system
-------------------------

If you want to add a new batch system, all you need to do is to implement the
abstract functions of ``BatchProcess`` for your system:

.. autoclass:: b2luigi.batch.processes.BatchProcess
    :members: get_job_status, start_job, kill_job
