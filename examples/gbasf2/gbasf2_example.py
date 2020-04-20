import os

import b2luigi
from b2luigi.basf2_helper.tasks import Basf2PathTask
from b2luigi.batch.processes.gbasf2 import get_unique_gbasf2_project_name
import example_mdst_analysis


class MyAnalysisTask(Basf2PathTask):
    # set the batch_system property to use the gbasf2 wrapper batch process for this task
    batch_system = "gbasf2"
    # Must define a prefix for the gbasf2 project name to submit to the grid.
    # b2luigi will then add a hash derived from the luigi parameters to create a unique project name.
    gbasf2_project_name_prefix = b2luigi.Parameter(significant=False)
    gbasf2_input_dataset = b2luigi.Parameter(hashed=True)
    # Define some more settings as class properties. Alternatively, they could
    # also be defined in the settings.json
    gbasf2_download_dir = "."
    gbasf2_cputime = 5  # expected time per job in minutes, we use a low one b/c we set max_event to a low value

    # As an example we define a cut range as a b2luigi ListParameter, so that we
    # can automatically create multiple tasks and thus multiple independent
    # gbasf2 projects for differents sets of cuts
    mbc_range = b2luigi.ListParameter(hashed=True)

    def create_path(self):
        return example_mdst_analysis.create_analysis_path(mbc_range=self.mbc_range)

    def output(self):
        """
        Define the output to be the directory into which the ``gb2_ds_get``
        command, which is wrapped by the gbasf2 batch system, downloads the
        dataset.  It is defined by the contatenation of the
        ``gbasf2_download_dir`` and ``gbasf2_project_name`` settings.
        """
        gbasf2_project_name = get_unique_gbasf2_project_name(self)
        gbasf2_dataset_dir = os.path.join(self.gbasf2_download_dir, gbasf2_project_name)
        return b2luigi.LocalTarget(gbasf2_dataset_dir)


class MasterTask(b2luigi.WrapperTask):
    """
    We use the MasterTask to require multiple analyse tasks with different input
    datasets and cut values.  For each parameter combination, a different gbasf2
    project will be submitted.
    """
    def requires(self):
        gbasf2_input_datasets = [
            "/belle/MC/release-04-00-03/DB00000757/MC13a/prod00009434/s00/e1003/4S/r00000/mixed/mdst/sub00",
            "/belle/MC/release-04-00-03/DB00000757/MC13a/prod00009435/s00/e1003/4S/r00000/charged/mdst/sub00",
        ]
        # if you want to iterate over different cuts, just add more values to this list
        mbc_lower_cuts = [5.15]
        for mbc_lower_cut in mbc_lower_cuts:
            for input_ds in gbasf2_input_datasets:
                yield MyAnalysisTask(
                    mbc_range=(mbc_lower_cut, 5.3),
                    gbasf2_project_name_prefix="luigiExampleMultiDS",
                    gbasf2_input_dataset=input_ds,
                    max_event=100,
                )


if __name__ == '__main__':
    b2luigi.process(MasterTask(), workers=2)
