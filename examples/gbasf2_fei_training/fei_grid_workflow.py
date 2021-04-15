from multiprocessing import Pool
import os
import glob

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
    stage = luigi.IntParameter()
    mode = luigi.Parameter()

    def output(self):

        for outname in fei_analysis_outputs[self.stage]:
            yield self.add_to_output(outname)

    def requires(self):

        if self.stage == -1:
            return [] # default implementation, as in the luigi.Task (if no requirements are present)
        else:
            yield PrepareInputsTask(
                mode="AnalysisInput",
                stage=self.stage - 1,
            )

    def create_path(self):

        luigi.set_setting("gbasf2_cputime",grid_cpu_time[self.stage])
        path = create_fei_path(filelist=[], cache=self.cache, monitor=self.monitor)
        return path

class MergeOutputsTask(luigi.Task):

    ncpus = luigi.IntParameter(significant=False) # to be used with setting 'local_cpus' in settings.json
    stage = luigi.IntParameter()
    mode = luigi.Parameter()

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
            # for some reason, only a one-element list: self.get_input_file_names(inname) (specific to gbasf2 output structure)
            cmds.append(f"analysis-fei-mergefiles {self.get_output_file_name(inname)} " + " ".join(glob.glob(os.path.join(self.get_input_file_names(inname)[0],"*.root"))))

        p = Pool(self.ncpus)
        p.map(shell_command,cmds)

class FEITrainingTask(luigi.Task):

    stage = luigi.IntParameter()
    mode = luigi.Parameter()

    def output(self):

        if self.stage == -1:
            self.add_to_output("no_training_needed.txt")
        else:
            pass

    def requires(self):

        # need merged mcParticlesCount.root from stage -1
        yield MergeOutputsTask(
            mode="Merging",
            stage=-1,
            ncpus=luigi.get_setting("local_cpus"),
        )

        # need merged training_input.root from current stage
        if self.stage > -1:

            yield MergeOutputsTask(
                mode="Merging",
                stage=self.stage,
                ncpus=luigi.get_setting("local_cpus"),
            )

        # need .xml training files from all previous stages of FEI training,
        # beginning with stage 1
        if self.stage > 0:

            for fei_stage in range(self.stage):

                yield FEITrainingTask(
                    mode="Training",
                    stage=fei_stage,
                )

class PrepareInputsTask(luigi.Task):

    stage = luigi.IntParameter()
    mode = luigi.Parameter()

    def requires(self):

        # need merged mcParticlesCount.root from stage -1
        yield MergeOutputsTask(
            mode="Merging",
            stage=-1,
            ncpus=luigi.get_setting("local_cpus"),
        )

        if self.stage > -1:
            # need .xml training files from current stage of FEI training
            yield FEITrainingTask(
                mode="Training",
                stage=self.stage,
            )

        # need .xml training files from all previous stages of FEI training,
        # beginning with stage 1
        if self.stage > 0:

            for fei_stage in range(self.stage):

                yield FEITrainingTask(
                    mode="Training",
                    stage=fei_stage,
                )

class ProduceStatisticsTask(luigi.WrapperTask):

    def requires(self):

        yield MergeOutputsTask(
            mode="Merging",
            stage=-1,
            ncpus=luigi.get_setting("local_cpus"),
        )


if __name__ == '__main__':
    main_task_instance = ProduceStatisticsTask()
    n_gbasf2_tasks = len(list(main_task_instance.requires()))
    luigi.process(main_task_instance, workers=n_gbasf2_tasks)
