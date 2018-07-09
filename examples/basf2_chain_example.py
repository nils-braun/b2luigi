import b2luigi
import basf2
import luigi


class SimulationTask(b2luigi.Basf2Task):
    n_events = luigi.IntParameter()
    event_type = luigi.Parameter()

    def create_path(self):
        import modularAnalysis
        import simulation
        import generators
        from ROOT import Belle2

        path = basf2.create_path()

        modularAnalysis.setupEventInfo(self.n_events, path)

        if self.event_type == 'Y4S':
            dec_file = Belle2.FileSystem.findFile('analysis/examples/tutorials/B2A101-Y4SEventGeneration.dec')
        elif self.event_type == 'Continuum':
            dec_file = Belle2.FileSystem.findFile('analysis/examples/tutorials/B2A102-ccbarEventGeneration.dec')
        else:
            raise ValueError(f"Event type {self.event_type} is not valid. It should be either 'Y(4S)' or 'Continuum'!")
        generators.add_evtgen_generator(path, 'signal', dec_file)

        modularAnalysis.loadGearbox(path)

        simulation.add_simulation(path)

        path.add_module('RootOutput', outputFileName=self.get_output_file_names()['simulation_full_output.root'])

        return path

    def output(self):
        yield self.add_to_output("simulation_full_output.root")


class ReconstructionTask(b2luigi.Basf2Task):
    n_events = luigi.IntParameter()
    event_type = luigi.Parameter()

    def create_path(self):
        import modularAnalysis
        import reconstruction

        path = basf2.create_path()

        path.add_module(
            'RootInput',
            inputFileNames=self.get_input_file_names()["simulation_full_output.root"]
        )

        modularAnalysis.loadGearbox(path)
        reconstruction.add_reconstruction(path)

        modularAnalysis.outputMdst(self.get_output_file_names()["reconstructed_output.root"], path=path)

        return path

    def output(self):
        yield self.add_to_output("reconstructed_output.root")

    def requires(self):
        yield self.clone(SimulationTask)


class AnalysisTask(b2luigi.Basf2Task):
    n_events = luigi.IntParameter()
    event_type = luigi.Parameter()

    def create_path(self):
        import modularAnalysis

        path = basf2.create_path()

        modularAnalysis.inputMdstList('default', self.get_input_file_names()["reconstructed_output.root"], path=path)

        modularAnalysis.fillParticleLists([('K+', 'kaonID > 0.1'), ('pi+', 'pionID > 0.1')], path=path)

        modularAnalysis.reconstructDecay('D0 -> K- pi+', '1.7 < M < 1.9', path=path)
        modularAnalysis.fitVertex('D0', 0.1, path=path)
        modularAnalysis.matchMCTruth('D0', path=path)

        modularAnalysis.reconstructDecay('B- -> D0 pi-', '5.2 < Mbc < 5.3', path=path)
        modularAnalysis.fitVertex('B+', 0.1, path=path)
        modularAnalysis.matchMCTruth('B-', path=path)

        modularAnalysis.variablesToNTuple(
            'D0',
            ['M', 'p', 'E', 'useCMSFrame(p)', 'useCMSFrame(E)',
             'daughter(0, kaonID)', 'daughter(1, pionID)', 'isSignal', 'mcErrors'],
            filename=self.get_output_file_names()["D_n_tuple.root"],
            path=path
        )

        modularAnalysis.variablesToNTuple(
            'B-',
            ['Mbc', 'deltaE', 'isSignal', 'mcErrors', 'M'],
            filename=self.get_output_file_names()["B_n_tuple.root"],
            path=path
        )

        return path

    def output(self):
        yield self.add_to_output("D_n_tuple.root")
        yield self.add_to_output("B_n_tuple.root")

    def requires(self):
        return self.clone(ReconstructionTask)


class MasterTask(b2luigi.core.helper_tasks.Basf2nTupleMergeTask):
    n_events = luigi.IntParameter()

    def requires(self):
        for event_type in ['Y4S', 'Continuum']:
            yield self.clone(AnalysisTask, event_type=event_type)


if __name__ == "__main__":
    b2luigi.process(MasterTask(n_events=500), workers=4)
