import hashlib
import os

import b2luigi
from b2luigi.basf2_helper.tasks import Basf2PathTask

import example_mdst_analysis


class MyAnalysisTask(Basf2PathTask):
    batch_system = "gbasf2"

    mbc_range = b2luigi.ListParameter(hashed=True)

    # In this example we define the project name and input dataset as luigi
    # parameters. Thus, they have to be defined when instantiating the task.
    # They can also be defined as normal class properties/attributes or static
    # property functions. But we have to make sure that the project name is
    # always unique for each new task instance with different parameters.
    gbasf2_project_name = b2luigi.Parameter(significant=False)
    gbasf2_input_dataset = b2luigi.Parameter(hashed=True)
    # Define some more settings as class properties. Alternatively, they could
    # also be defined in the settings.json
    gbasf2_download_dir = "."
    gbasf2_cputime = 5  # expected time per job in minutes
    gbasf2_priority = 5

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
        max_event = 100
        # create two analysis task for two different MBC cuts. Each will be its own gbasf2 project
        for mbc_lower_cut in [5.1, 5.2]:
            mbc_range = (mbc_lower_cut, 5.3)
            parameter_hash = hashlib.md5(f"{max_event}_{mbc_range}_{gbasf2_input_dataset}".encode()).hexdigest()[0:10]
            unique_project_name = f"luigiExample{parameter_hash}"
            yield MyAnalysisTask(
                mbc_range=mbc_range,
                gbasf2_project_name=unique_project_name,
                gbasf2_input_dataset=gbasf2_input_dataset,
                max_event=100,
            )


if __name__ == '__main__':
    b2luigi.process(MasterTask(), batch=True, workers=1)
