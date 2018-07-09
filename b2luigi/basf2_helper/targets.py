import b2luigi
import ROOT


class ROOTLocalTarget(b2luigi.LocalTarget):
    def exists(self):
        if not super().exists():
            return False

        path = self.path
        tfile = ROOT.TFile.Open(path)
        return tfile and len(tfile.GetListOfKeys()) > 0