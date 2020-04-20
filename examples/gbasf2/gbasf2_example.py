import os

import b2luigi
from b2luigi.basf2_helper.tasks import Basf2PathTask

import example_mdst_analysis


class MyAnalysisTask(Basf2PathTask):
    batch_system = "gbasf2"

    mbc_range = b2luigi.ListParameter(hashed=True)

    # common name for all tasks from this class
    gbasf2_project_name_prefix = b2luigi.Parameter(significant=False)
    # calculate specific unique project name for this instance of this task, so
    # that each change in b2luigi parameters results in a different gbasf2
    # project
    @property
    def gbasf2_project_name(self):
        task_id_hash = self.task_id.split("_")[-1]
        return self.gbasf2_project_name_prefix + task_id_hash

    gbasf2_input_dataset = b2luigi.Parameter(hashed=True)
    # Define some more settings as class properties. Alternatively, they could
    # also be defined in the settings.json
    gbasf2_download_dir = "."
    gbasf2_cputime = 5  # expected time per job in minutes

    def create_path(self):
        return example_mdst_analysis.create_analysis_path(mbc_range=self.mbc_range)

    def output(self):
        """
        Define the output to be the directory into which the ``gb2_ds_get``
        command, which is wrapped by the gbasf2 batch system, downloads the
        dataset.  It is defined by the contatenation of the
        ``gbasf2_download_dir`` and ``gbasf2_project_name`` settings.

        Warning: That means that the job is also done, if the project directory
        had been created, but is not containing any or all files.  The files are
        downloaded only if the project is fully successful, so I am relying on
        the download not to fail.  If the target exists but you want to re-run
        the task, just delete it by hand.
        """
        return b2luigi.LocalTarget(os.path.join(self.gbasf2_download_dir, self.gbasf2_project_name))


class MasterTask(b2luigi.WrapperTask):
    def requires(self):
        gbasf2_input_dataset = os.path.join("/belle/MC/release-04-00-03/DB00000757/MC13a/prod00009434/s00/e1003/4S/",

                                            "r00000/mixed/mdst/sub00/mdst_000255_prod00009434_task10020000255.root")
        # create two analysis task for two different MBC cuts. Each will be its own gbasf2 project
        for mbc_lower_cut in [5.15, 5.2]:
            yield MyAnalysisTask(
                mbc_range=(mbc_lower_cut, 5.3),
                gbasf2_project_name_prefix="luigiExample",
                gbasf2_input_dataset=gbasf2_input_dataset,
                max_event=100,
            )


if __name__ == '__main__':
    b2luigi.process(MasterTask(), workers=2)
