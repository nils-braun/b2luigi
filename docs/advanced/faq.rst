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
a own :meth:`get_log_file_dir()` method of the task class must be implemented. This method
must return a directory path for the stdout and the stderr files, for example:

.. code-block:: python

    class MyBatchTask(b2luigi.Task):
        ...
        def get_log_file_dir(self):
            filename = os.path.realpath(sys.argv[0])
            path = os.path.join(os.path.dirname(filename), "logs")
            return path

``b2luigi`` will use this method if it is defined and write the log output in the respective
files. Be careful, though, as these log files will of course be overwritten if more than one
task receive the same paths to write to!


Can I exclude one job from batch processing
-------------------------------------------

The setting ``batch_system`` defines which submission method is used for scheduling
your tasks when using ``batch=True`` or ``--batch``.
In most cases, you set your ``batch_system`` globally (e.g. in a ``settings.json``)
file and start all your tasks with ``--batch`` or ``batch=True``.
If you want a single task to run only locally (e.g. because of constraints in
the batch farm) you can set the ``batch_system`` only for this job by adding a member to this task:

.. code-block:: python

    class MyLocalTask(b2luigi.Task):
        batch_system = "local"

        def run(self):
            ...

How do I handle parameter values which include "/" (or other unusual characters)?
---------------------------------------------------------------------------------

``b2luigi`` automatically generates the filenames for your output or log files out of
the current tasks values in the form::

    <result-path>/param1=value1/param2=value2/..../filename.ext

The values are given by the serialisation of your parameter, which is basically its string representation.
Sometimes, this representation may include characters not suitable for their usage as a path name,
e.g. "/".
Especially when you use a :obj:`DictParameter` or a :obj:`ListParameter`, you might not
want to have its value in your output.
Also, if you have credentials in the parameter (what you should never do of course!), you do not
want to show them to everyone.

When using a parameter in `b2luigi` (or any of its derivatives), they have a new flag called ``hashed``
in their constructor, which makes the path creation only using a hashed version of your parameter value.

For example will this task::

    class MyTask(b2luigi.Task):
        my_parameter = b2luigi.ListParameter(hashed=True)

        def run(self):
            with open(self.get_output_file_name("test.txt"), "w") as f:
                f.write("test")

        def output(self):
            yield self.add_to_output("test.txt")


    if __name__ == "__main__":
        b2luigi.process(MyTask(my_parameter=["Some", "strange", "items", "with", "bad / signs"]))

create a file called ``my_parameter=hashed_08928069d368e4a0f8ac02a0193e443b/test.txt`` in your output folder
instead of using the list value.


What does the ValueError "The task id {task.task_id} to be executed..." mean?
-----------------------------------------------------------------------------

The `ValueError` exception `The task id <task_id> to be executed by this batch worker does
not exist in the locally reproduced task graph.` is thrown by ``b2luigi`` batch workers if
the task that should have been executed by this batch worker does not exist in the task
graph reproduced by the batch worker. This means that the task graph produced by the initial
``b2luigi.process`` call and the one reproduced in the batch job differ from each other.
This can be caused by a non-deterministic behavior of your dependency graph generation, such
as a random task parameter.
