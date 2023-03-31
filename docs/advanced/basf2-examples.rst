.. _`basf2-example-label`:

Belle II specific examples
==========================

The following examples are not of interest to the general audience, but only for basf2 users.


Running at the NAF
------------------

The environment on the workers is different than on the scheduling machine, so we can not
just copy the environment variables as on KEKCC.

You can use setup script (e.g. called ``setup_basf2.sh``) with the following content

.. code-block:: bash

    source /cvmfs/belle.cern.ch/tools/b2setup release-XX-XX-XX

All you have to do is specify the setup script as the ``env_script`` setting and also set the
executable explicitly.

.. code-block:: python

    from time import sleep
    import os

    import b2luigi


    class MyTask(b2luigi.Task):
        parameter = b2luigi.IntParameter()

        def output(self):
            yield self.add_to_output("test.txt")

        def run(self):
            sleep(self.parameter)

            with open(self.get_output_file_name("test.txt"), "w") as f:
                f.write("Test")


    class Wrapper(b2luigi.Task):
        def requires(self):
            for i in range(10):
                yield MyTask(parameter=i)

        def output(self):
            yield self.add_to_output("test.txt")

        def run(self):
            with open(self.get_output_file_name("test.txt"), "w") as f:
                f.write("Test")


    if __name__ == '__main__':
        # Choose htcondor as our batch system
        b2luigi.set_setting("batch_system", "htcondor")

        # Setup the correct environment on the workers
        b2luigi.set_setting("env_script", "setup_basf2.sh")

        # Most likely your executable from the submission node is not the same on
        # the worker node, so specify it explicitly
        b2luigi.set_setting("executable", ["python3"])

        # Where to store the results
        b2luigi.set_setting("result_dir", "results")

        b2luigi.process(Wrapper(), batch=True, workers=100)

Of course it is also possible to set those settings in the ``settings.json`` or as task-specific parameters.
Please check out :func:`b2luigi.get_setting` for more information.

Please note that the called script as well as the results folder need to be accessible from both
the scheduler and the worker machines.
If needed, you can also include more setup steps in the source script.

Running at KEKCC
----------------

KEKCC uses LSF as the batch system. As this is the default for ``b2luigi`` there is
nothing you need to do.

nTuple Generation
-----------------

.. code-block:: python

    import b2luigi as luigi
    from b2luigi.basf2_helper import Basf2PathTask, Basf2nTupleMergeTask

    import basf2

    import modularAnalysis


    class AnalysisTask(Basf2PathTask):
        experiment_number = luigi.IntParameter()
        run_number = luigi.IntParameter()
        mode = luigi.Parameter()
        file_number = luigi.IntParameter()

        def output(self):
            # Define the outputs here
            yield self.add_to_output("D_n_tuple.root")
            yield self.add_to_output("B_n_tuple.root")

        def create_path(self):
            # somehow create filenames from parameters
            # self.experiment_number, self.run_number,
            # self.mode and self.file_number
            # (parameters just examples)
            input_file_names = ..

            path = basf2.Path()
            modularAnalysis.inputMdstList('default', input_file_names, path=path)

            # Now fill your particle lists, just examples
            modularAnalysis.fillParticleLists([('K+', 'kaonID > 0.1'), ('pi+', 'pionID > 0.1')],
                                              path=path)
            modularAnalysis.reconstructDecay('D0 -> K- pi+', '1.7 < M < 1.9', path=path)
            modularAnalysis.fitVertex('D0', 0.1, path=path)
            modularAnalysis.matchMCTruth('D0', path=path)
            modularAnalysis.reconstructDecay('B- -> D0 pi-', '5.2 < Mbc < 5.3', path=path)
            modularAnalysis.fitVertex('B+', 0.1, path=path)
            modularAnalysis.matchMCTruth('B-', path=path)

            # When exporting, use the function get_output_file_name()
            modularAnalysis.variablesToNtuple('D0',
                                            ['M', 'p', 'E', 'useCMSFrame(p)', 'useCMSFrame(E)',
                                            'daughter(0, kaonID)', 'daughter(1, pionID)', 'isSignal', 'mcErrors'],
                                            filename=self.get_output_file_name("D_n_tuple.root"),
                                            path=path)
            modularAnalysis.variablesToNtuple('B-',
                                            ['Mbc', 'deltaE', 'isSignal', 'mcErrors', 'M'],
                                            filename=self.get_output_file_name("B_n_tuple.root"),
                                            path=path)
            return path


    class AnalysisWrapperTask(luigi.WrapperTask):
        def requires(self):
            # somehow loop over the runs, experiment etc.
                yield self.clone(AnalysisTask, experiment_number=...)


    if __name__ == "__main__":
        luigi.process(AnalysisWrapperTask(), workers=500)


Standard Simulation, Reconstruction and some nTuple Generation
--------------------------------------------------------------

.. literalinclude:: ../../examples/basf2/basf2_chain_example.py
