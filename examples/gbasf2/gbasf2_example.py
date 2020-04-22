import b2luigi
from b2luigi.basf2_helper.tasks import Basf2PathTask
import example_mdst_analysis


class MyAnalysisTask(Basf2PathTask):
    # set the batch_system property to use the gbasf2 wrapper batch process for this task
    batch_system = "gbasf2"
    # Must define a prefix for the gbasf2 project name to submit to the grid.
    # b2luigi will then add a hash derived from the luigi parameters to create a unique project name.
    gbasf2_project_name_prefix = b2luigi.Parameter(significant=False)
    gbasf2_input_dataset = b2luigi.Parameter(hashed=True)

    # Example luigi cut parameter to facilitate starting multiple projects for different cut values
    mbc_range = b2luigi.ListParameter(hashed=True)

    def create_path(self):
        return example_mdst_analysis.create_analysis_path(mbc_range=self.mbc_range)

    def output(self):
        yield self.add_to_output(self.gbasf2_project_name_prefix)


class MasterTask(b2luigi.WrapperTask):
    """
    We use the MasterTask to be able to require multiple analyse tasks with
    different input datasets and cut values. For each parameter combination, a
    different gbasf2 project will be submitted.
    """
    def requires(self):
        gbasf2_input_datasets = [
            "/belle/MC/release-04-00-03/DB00000757/MC13a/prod00009434/s00/e1003/4S/r00000/mixed/mdst/sub00",
            "/belle/MC/release-04-00-03/DB00000757/MC13a/prod00009435/s00/e1003/4S/r00000/charged/mdst/sub00",
        ]
        # if you want to iterate over different cuts, just add more values to this list
        mbc_lower_cuts = [5.15, 5.2]
        for mbc_lower_cut in mbc_lower_cuts:
            for input_ds in gbasf2_input_datasets:
                yield MyAnalysisTask(
                    mbc_range=(mbc_lower_cut, 5.3),
                    gbasf2_project_name_prefix="luigiExampleMultiDS",
                    gbasf2_input_dataset=input_ds,
                    max_event=100,
                )


if __name__ == '__main__':
    master_task_instance = MasterTask()
    n_gbasf2_tasks = len(list(master_task_instance.requires()))
    b2luigi.process(master_task_instance, workers=n_gbasf2_tasks)
