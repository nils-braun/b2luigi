import os

import b2luigi
from b2luigi.basf2_helper.tasks import Basf2PathTask
from b2luigi.core.settings import get_setting

import example_mdst_analysis


class MyAnalysisTask(Basf2PathTask):
    # We define some settings for the task. These can also alternatively be set for all tasks in the settings.json
    batch_system = "gbasf2"
    # note: make sure to use a unique project name not used before
    gbasf2_project_name = "b2luigi_gbasf2_example"
    gbasf2_download_dir = "."
    gbasf2_cputime = 5  # expected time per job in minutes
    gbasf2_priority = 5
    gbasf2_input_dataset = os.path.join("/belle/MC/release-04-00-03/DB00000757/MC13a/prod00009434/s00/e1003/4S/",
                                        "r00000/mixed/mdst/sub00/mdst_000255_prod00009434_task10020000255.root")
    max_event = 100  # limit number of events for testing

    def create_path(self):
        return example_mdst_analysis.create_analysis_path()

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
        return b2luigi.LocalTarget(os.path.join(self.gbasf2_download_dir, MyAnalysisTask.gbasf2_project_name))


if __name__ == '__main__':
    b2luigi.process(MyAnalysisTask(), batch=True)
