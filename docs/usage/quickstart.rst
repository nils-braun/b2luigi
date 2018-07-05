.. _quick-start-label:

Quick Start
===========

After having installed b2luigi (see :ref:`installation-label`), we dive directly into the first example:

.. code-block:: python
    :linenos:

    import b2luigi

    class ReconstructionTask(b2luigi.Task):
        number = b2luigi.IntParameter()

        def output(self):
            yield self.add_to_output("output.txt")

        def run(self):
            output_file_name = self.get_output_file_names()["output.txt"]

            if self.number == 3:
                raise ValueError("On purpose")

            with open(output_file_name, "w") as f:
                f.write(f"This is a test with number {self.number}\n")

    class BossTask(b2luigi.Task):
        def requires(self):
            for i in range(10):
                yield ReconstructionTask(number=i)

        def output(self):
            yield self.add_to_output("boss.txt")

        def run(self):
            output_file_name = self.get_output_file_names()["boss.txt"]

            with open(output_file_name, "w") as f:
                f.write(f"Boss!\n")

    if __name__ == "__main__":
        b2luigi.process(BossTask(), workers=100)

After the import in line 1, two ``Tasks`` are defined in lines 3-16 and 18-30.

The ``ReconstructionTask`` has a single parameter of type ``int`` defined in line 4.
Its job is, to create a new output file called ``output.txt`` and write the line

    This is a test with number <number>

where <number> is set to the parameter into this file.
Every ``Task`` needs to define the output files it writes (and every task needs to have at least a single output file).
If your task does not write out output files, you need to inherit from ``b2luigi.WrapperTask`` instead of ``b2luigi.Task``.

For testing reasons, we fail the task with the number 3 on purpose later.

The ``BossTask`` defined in lines 18-30 requires the ``ReconstructionTask`` to run before it will start (more precise:
the ones with the numbers 0 to 9).
It will then also output a file called ``boss.txt`` with some text in it.
It has no parameters.

The last line 33 just defines what our root task it (``BossTask``) and how many tasks we want to run in parallel at maximum
(100 in this case, but we will not need that much).

We can now save this file as ``simple-task.py`` and start processing it:

.. code-block:: bash

    basf2 simple-task.py

This will run the tasks locally. It will start the 10 ``ReconstructionTasks`` first and wait until all of them are finished.
In principle, it would then start the ``BossTask`` afterwards.
But as we have an error in one of our tasks, it will stop here and tell you the result:

.. code::

    ===== Luigi Execution Summary =====

    Scheduled 11 tasks of which:
    * 9 present dependencies were encountered:
        - 9 MyTask(git_hash=75446bc854d14f5bf1a7a7ed95a408d7945d9d45, number=0) ...
    * 1 failed:
        - 1 MyTask(git_hash=75446bc854d14f5bf1a7a7ed95a408d7945d9d45, number=3)
    * 1 were left pending, among these:
        * 1 had failed dependencies:
            - 1 BossTask(git_hash=75446bc854d14f5bf1a7a7ed95a408d7945d9d45)

    This progress looks :( because there were failed tasks

    ===== Luigi Execution Summary =====

You can now repair the script and delete lines 12 and 13 and rerun. The result will look something like this:

.. code::

    ===== Luigi Execution Summary =====

    Scheduled 11 tasks of which:
    * 9 present dependencies were encountered:
        - 9 MyTask(git_hash=75446bc854d14f5bf1a7a7ed95a408d7945d9d45, number=0) ...
    * 2 ran successfully:
        - 1 BossTask(git_hash=75446bc854d14f5bf1a7a7ed95a408d7945d9d45)
        - 1 MyTask(git_hash=75446bc854d14f5bf1a7a7ed95a408d7945d9d45, number=3)

    This progress looks :) because there were no failed tasks or missing external dependencies

    ===== Luigi Execution Summary =====

The script ran though as expected, but there is something more to see here: ``9 present dependencies were encountered:``
Luigi is seeing, that some tasks have already produced output files and do not need to rerun them!
This can save a lot of computing time!

But were are the output files now? You might see two new folders in your current working directly

.. code:: bash

    $ ls
    git_hash=75446bc854d14f5bf1a7a7ed95a408d7945d9d45
    logs
    simple-task.py

The logs folder stores the logs of every execution of a single tasks.
The output files are stored in folder for each task differently, which are built out of all parameters of the task and
the git hash of the basf2 release you are using. So for example the ``ReconstructionTask`` with number 2 will write
its output to

    git_hash=_<hash>/number=2/output.txt

and its logs to

    logs/git_hash=_<hash>/number=2/ReconstructionTask_(stderr/stdout)

This is all controllable by :ref:`settings-label`.

If this works as expected, you can go on with scheduling jobs on the KEKCC queue in :ref:`batch-label`.





