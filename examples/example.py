import time

import b2luigi
import basf2
import luigi

class GenerationTask(b2luigi.SimplifiedOutputBasf2Task):
    number = luigi.IntParameter()

    def create_path(self):
        path = basf2.create_path()

        path.add_module("EventInfoSetter", evtNumList=[self.number])
        path.add_module("RootOutput", outputFileName=f"generated_{self.number}.root")

        return path

    def run_local(self):
        super().run_local()
        time.sleep(10)


class Killer(basf2.Module):
    def event(self):
        raise ValueError


class ReconstructionTask(b2luigi.SimplifiedOutputBasf2Task):
    number = luigi.IntParameter()

    def requires(self):
        return self.clone(GenerationTask)

    def create_path(self):
        path = basf2.create_path()

        path.add_module("RootInput", inputFileNames=self.get_input_file_names())

        if self.number == 3:
            path.add_module(Killer())

        path.add_module("RootOutput", outputFileName=f"reconstruction_{self.number}.root")

        return path

    def run_local(self):
        super().run_local()
        time.sleep(10)


class MainTask(luigi.WrapperTask):
    def requires(self):
        return [ReconstructionTask(number=1), ReconstructionTask(number=3)]


if __name__ == "__main__":
    b2luigi.process(MainTask())
