from multiprocessing import Pool
import os

import b2luigi as luigi
from b2luigi.basf2_helper.tasks import Basf2PathTask
from b2luigi.basf2_helper.utils import get_basf2_git_hash

from B_generic_train import create_fei_path

def shell_command(cmd):
    os.system(cmd)

# ballpark CPU times for the FEI stages executed on grid. Format: <stage-number> : <minutes> 
grid_cpu_time = {
    -1 : 10,
    0  : 20,
    1  : 30,
    2  : 40,
    3  : 8  * 60,
    4  : 16 * 60,
    5  : 24 * 60,
    6  : 36 * 60,
}

fei_analysis_outputs = {}
fei_analysis_outputs[-1] = ["mcParticlesCount.root"]
for i in range(6):
    fei_analysis_outputs[i] = ["training_input.root"]
fei_analysis_outputs[6] = [
    "Monitor_FSPLoader.root",
    "Monitor_Final.root",
    "Monitor_ModuleStatistics.root",
    "Monitor_PostReconstruction_AfterMVA.root",
    "Monitor_PostReconstruction_AfterRanking.root",
    "Monitor_PostReconstruction_BeforePostCut.root",
    "Monitor_PostReconstruction_BeforeRanking.root",
    "Monitor_PreReconstruction_AfterRanking.root",
    "Monitor_PreReconstruction_AfterVertex.root",
    "Monitor_PreReconstruction_BeforeRanking.root",
]


class FEIAnalysisTask(Basf2PathTask):
    batch_system = "gbasf2"

    git_hash = luigi.Parameter(hashed=True,default=get_basf2_git_hash(),significant=False)

    gbasf2_project_name_prefix = luigi.Parameter(significant=False)
    gbasf2_input_dataset = luigi.Parameter(hashed=True,significant=False)

    cache = luigi.IntParameter(significant=False)
    monitor = luigi.BoolParameter(significant=False)
    mode = luigi.Parameter()
    stage = luigi.IntParameter()
    

    def output(self):
        for outname in fei_analysis_outputs[self.stage]:
            yield self.add_to_output(outname)

    def create_path(self):
        luigi.set_setting("gbasf2_cputime",grid_cpu_time[self.stage])
        path = create_fei_path(filelist=[], cache=self.cache, monitor=self.monitor)
        return path

class MergeOutputsTask(luigi.Task):

    ncpus = luigi.IntParameter(significant=False) # to be used with setting 'local_cpus' in settings.json
    mode = luigi.IntParameter()
    stage = luigi.Parameter()

    def output(self):
        for outname in fei_analysis_outputs[self.stage]:
            yield self.add_to_output(outname)

    def requires(self):
        cache = -1 if self.stage == -1 else 0
        monitor = True if self.stage == 6 else False
        yield FEIAnalysisTask(
            cache=cache,
            monitor=monitor,
            mode="TrainingInput",
            stage=self.stage,
            gbasf2_project_name_prefix=luigi.get_setting("gbasf2_project_name_prefix"),
            gbasf2_input_dataset=luigi.get_setting("gbasf2_input_dataset"),
        )

    def run(self):
        cmds = []
        for inname in fei_analysis_outputs[self.stage]:
            cmds.append(f"analysis-fei-mergefiles {inname} " + " ".join(self.get_input_file_names(inname)))

        p = Pool(self.ncpus)
        p.map(shell_command,cmds)

class ProduceStatisticsTask(luigi.WrapperTask):

    def requires(self):
        input_dataset = "/work/akhmet/Belle2Project/flist_test.txt"

        yield FEIAnalysisTask(
            cache=-1,
            monitor=False,
            mode="TrainingInput",
            stage=-1,
            gbasf2_project_name_prefix=luigi.get_setting("gbasf2_project_name_prefix"),
            gbasf2_input_dataset=luigi.get_setting("gbasf2_input_dataset"),
        )


if __name__ == '__main__':
    main_task_instance = ProduceStatisticsTask()
    n_gbasf2_tasks = len(list(main_task_instance.requires()))
    luigi.process(main_task_instance, workers=n_gbasf2_tasks)
