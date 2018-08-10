.. _faq-label:

FAQ
===

Can I specify my own paths for the log files for tasks running on a batch system?
---------------------------------------------------------------------------------

``b2luigi`` will automatically create log files for the ``stdout`` and ``stderr``
output of a task processed on a batch system. The paths of these log files are defined
relative to the location of the executed python file and contain the parameter of
the task.
In some cases one might one to specify other paths for the log files. To achieve this,
a own :meth:`get_log_files()` method of the task class must be implemented. This method
must return a tuple of the paths for the stdout and the stderr files, for example:

.. code-block:: python

    class MyBatchTask(b2luigi.Task):
        ...
        def get_log_files(self):
            filename = os.path.realpath(sys.argv[0])
            stdout_path = os.path.join(os.path.dirname(filename), "logs", "simple_stdout")
            stderr_path = os.path.join(os.path.dirname(filename), "logs", "simple_stderr")
            return stdout_path, stderr_path

``b2luigi`` will use this method if it is defined and write the log output in the respective
files. Be careful, though, as these log files will of course be overwritten if more than one
task receive the same paths to write to!


What does the ValueError "The task id {task.task_id} to be executed..." mean?
-----------------------------------------------------------------------------

The `ValueError` exception `The task id <task_id> to be executed by this batch worker does
not exist in the locally reproduced task graph.` is thrown by ``b2luigi`` batch workers if
the task that should have been executed by this batch worker does not exist in the task
graph reproduced by the batch worker. This means that the task graph produced by the initial
``b2luigi.process`` call and the one reproduced in the batch job differ from each other.
This can be caused by a non-deterministic behavior of your dependency graph generation, such
as a random task parameter.
