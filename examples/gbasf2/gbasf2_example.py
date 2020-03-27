from os.path import join

import b2luigi
from b2luigi.basf2_helper.tasks import Basf2PathTask

import mdst_analysis


class MyAnalysisTask(Basf2PathTask):
    batch_system = "gbasf2"
    gbasf2_release = "light-1912-icarus"
    gbasf2_project_name = "b2luigi_gbasf2_example"
    max_event = 200  # limit number of events for testing
    gbasf2_cputime = 30  # expected time in minutes

    gbasf2_additional_params = "--unique"
    gbasf2_input_dataset = join("/belle/MC/release-04-00-03/DB00000757/MC13a/prod00009434/s00/e1003/4S/",
                                "r00000/mixed/mdst/sub00/mdst_000255_prod00009434_task10020000255.root")

    def create_path(self):
        return mdst_analysis.create_analysis_path()

    def output(self):
        return b2luigi.LocalTarget(MyAnalysisTask.gbasf2_project_name)


if __name__ == '__main__':
    b2luigi.process(MyAnalysisTask(), batch=True)
