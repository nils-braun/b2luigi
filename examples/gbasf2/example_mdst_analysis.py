#!/usr/bin/env python3
"""
This file is not a standalone example, but meant to provide a simple example
analysis path be imported from gbasf2_example.py
"""

import basf2
import modularAnalysis as mA
import vertex as vx
from stdCharged import stdK, stdPi
from variables import variables as vm


def create_analysis_path(
        b_ntuple_filename="B_ntuple.root",
        d_ntuple_filename="D_ntuple.root",
        mbc_range=(5.2, 5.3),
):
    """
    Example of a minimal reconstruction with a cut as a changeable function
    parameter, adapted from code in the ``B2T_Basics_3_FirstAnalysis.ipynb``
    notebook from b2 starter kit.
    """
    path = basf2.create_path()
    # this local inputMdstList will only be used when this steerig file is run locally, gbasf2 overrides it
    local_input_files = ["/group/belle2/dataprod/MC/MC13a/prod00009434/s00/e1003/4S/r00000/mixed/mdst/sub00/mdst_000001_prod00009434_task10020000001.root"]
    mA.inputMdstList(
        environmentType="default",
        filelist=local_input_files,
        path=path,
    )
    stdK("higheff", path=path)
    stdPi("higheff", path=path)
    mA.reconstructDecay('D0:Kpi -> K-:higheff pi+:higheff', '1.7 < M < 1.9', path=path)
    # use try except to have this code work for both the old and new function names for the tree fit
    mA.matchMCTruth('D0:Kpi', path=path)
    mA.reconstructDecay('B- -> D0:Kpi pi-:higheff', f"{mbc_range[0]} < Mbc < {mbc_range[1]}", path=path)
    try:
        vx.treeFit('B+', 0.1, path=path)
    except AttributeError:
        vx.vertexTree('B+', 0.1, path=path)
    mA.setAnalysisConfigParams({"mcMatchingVersion": "BelleII"}, path)
    mA.matchMCTruth('B-', path=path)
    vm.addAlias("p_cms", "useCMSFrame(p)")  # include aliases to test if they work
    vm.addAlias("E_cms", "useCMSFrame(E)")
    mA.variablesToNtuple('D0:Kpi', ['M', 'p', 'E', 'E_cms', 'p_cms', 'daughter(0, kaonID)',
                                    'daughter(1, pionID)', 'isSignal', 'mcErrors'],
                         filename=d_ntuple_filename, treename="D", path=path)
    mA.variablesToNtuple('B-', ['Mbc', 'deltaE', 'isSignal', 'mcErrors', 'M'],
                         filename=b_ntuple_filename, treename="B", path=path)
    return path


if __name__ == '__main__':
    # Use this to execute the analysis path if this file is called as a main
    # file. It can be used to test the analysis path independently of the gbasf2
    # luigi task. But if this module is only imported, the following is not executed.
    path = create_analysis_path()
    basf2.print_path(path)
    basf2.process(path)
