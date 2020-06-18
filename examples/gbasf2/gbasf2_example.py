from os.path import join

import b2luigi
from b2luigi.basf2_helper.tasks import Basf2PathTask

import example_mdst_analysis


class MyAnalysisTask(Basf2PathTask):
    # set the batch_system property to use the gbasf2 wrapper batch process for this task
    batch_system = "gbasf2"
    # Must define a prefix for the gbasf2 project name to submit to the grid.
    # b2luigi will then add a hash derived from the luigi parameters to create a unique project name.
    gbasf2_project_name_prefix = b2luigi.Parameter()
    gbasf2_input_dataset = b2luigi.Parameter(hashed=True)
    # Example luigi cut parameter to facilitate starting multiple projects for different cut values
    mbc_lower_cut = b2luigi.IntParameter()

    def create_path(self):
        mbc_range = (self.mbc_lower_cut, 5.3)
        return example_mdst_analysis.create_analysis_path(
            d_ntuple_filename="D_ntuple.root",
            b_ntuple_filename="B_ntuple.root",
            mbc_range=mbc_range
        )

    def output(self):
        yield self.add_to_output("D_ntuple.root")
        yield self.add_to_output("B_ntuple.root")


class MasterTask(b2luigi.WrapperTask):
    """
    We use the MasterTask to be able to require multiple analyse tasks with
    different input datasets and cut values. For each parameter combination, a
    different gbasf2 project will be submitted.
    """
    def requires(self):
        input_dataset = join("/belle/MC/release-04-00-03/DB00000757/MC13a/prod00009434/s00/e1003/",
                             "4S/r00000/mixed/mdst/sub00/mdst_000255_prod00009434_task10020000255.root")
        # if you want to iterate over different cuts, just add more values to this list
        mbc_lower_cuts = [5.15, 5.2]
        for mbc_lower_cut in mbc_lower_cuts:
            yield MyAnalysisTask(
                mbc_lower_cut=mbc_lower_cut,
                gbasf2_project_name_prefix="luigiExample",
                gbasf2_input_dataset=input_dataset,
                max_event=100,
            )


if __name__ == '__main__':
    main_task_instance = MasterTask()
    n_gbasf2_tasks = len(list(main_task_instance.requires()))
    b2luigi.process(main_task_instance, workers=n_gbasf2_tasks)
