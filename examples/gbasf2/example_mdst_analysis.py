#!/usr/bin/env python3
"""
This file is not a standalone example, but meant to provide a simple example
analysis path be imported from gbasf2_example.py
"""

import basf2
import modularAnalysis as mA
import vertex as vx
from stdCharged import stdK, stdPi


def create_analysis_path(mbc_cut_string='5.2 < Mbc < 5.3'):
    """
    Example of a minimal reconstruction with a cut as a changeable function
    parameter, adapted from code in the ``B2T_Basics_3_FirstAnalysis.ipynb``
    notebook from b2 starter kit.
    """
    path = basf2.create_path()
    # define some testing input on KEKCC, though gbasf2 will automatically
    # replace it by the correct input files from the input data set on the grid
    mA.inputMdstList(
        environmentType="default",
        filelist=["/group/belle2/dataprod/MC/MC13a/prod00009434/s00/e1003/4S/r00000/mixed/mdst/sub00/mdst_000001_prod00009434_task10020000001.root"],
        path=path,
    )
    stdK("higheff", path=path)
    stdPi("higheff", path=path)
    mA.reconstructDecay('D0:Kpi -> K-:higheff pi+:higheff', '1.7 < M < 1.9', path=path)
    # use try except to have this code work for both the old and new function names for the tree fit
    mA.matchMCTruth('D0:Kpi', path=path)
    mA.reconstructDecay('B- -> D0:Kpi pi-:higheff', mbc_cut_string, path=path)
    try:
        vx.treeFit('B+', 0.1, path=path)
    except AttributeError:
        vx.vertexTree('B+', 0.1, path=path)
    mA.setAnalysisConfigParams({"mcMatchingVersion": "BelleII"}, path)
    mA.matchMCTruth('B-', path=path)
    mA.variablesToNtuple('D0:Kpi', ['M', 'p', 'E', 'useCMSFrame(p)', 'useCMSFrame(E)', 'daughter(0, kaonID)',
                                    'daughter(1, pionID)', 'isSignal', 'mcErrors'],
                         filename='ntuple.root', treename="D", path=path)
    mA.variablesToNtuple('B-', ['Mbc', 'deltaE', 'isSignal', 'mcErrors', 'M'],
                         filename="ntuple.root", treename="B", path=path)
    return path


if __name__ == '__main__':
    # Use this to execute the analysis path if this file is called as a mine
    # file. It can be used to test the analysis path independently of the gbasf2
    # luigi task. But if this module is only imported, the followinis not executed.
    path = create_analysis_path()
    basf2.print_path(path)
    basf2.process(path)
