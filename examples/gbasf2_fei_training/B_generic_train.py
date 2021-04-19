#!/usr/bin/env python3

# William Sutcliffe 2019

import fei
import basf2 as b2
import modularAnalysis as ma


# Get FEI default channels.
# Utilise the arguments to toggle on and off certain channels


def get_particles():
    return fei.get_default_channels()


def create_fei_path(filelist=[], cache=0, monitor=False, verbose=False):

    # create path
    path = b2.create_path()

    # Load input ROOT file
    ma.inputMdstList(environmentType='default',
                     filelist=filelist,
                     # filelist=['/ceph/akhmet/test_fei_gbasf2_example_input/sub00/mdst_000001_prod00014140_task00000001.root'],
                     path=path)

    # datsets (example with BGx0):
    # /belle/MC/release-04-00-03/DB00000757/MC13a/prod00014140/s00/e0000/4S/r00000/mixed/mdst (51 Mio)
    # /belle/MC/release-04-00-03/DB00000757/MC13a/prod00014141/s00/e0000/4S/r00000/mixed/mdst (51 Mio)
    # /belle/MC/release-04-00-03/DB00000757/MC13a/prod00014142/s00/e0000/4S/r00000/charged/mdst (51 Mio)
    # /belle/MC/release-04-00-03/DB00000757/MC13a/prod00014143/s00/e0000/4S/r00000/charged/mdst (51 Mio)

    particles = get_particles()

    # Set up FEI configuration specifying the FEI prefix

    # for creation of mcParticlesCount.root
    # configuration = fei.config.FeiConfiguration(prefix='FEI_TEST', training=True, monitor=False,  cache=-1)

    # configuration = fei.config.FeiConfiguration(prefix='FEI_TEST', training=True, monitor=False, cache=0)

    # for final stage 6
    # configuration = fei.config.FeiConfiguration(prefix='FEI_TEST', training=True, monitor=True, cache=0)

    configuration = fei.config.FeiConfiguration(prefix='FEI_TEST', training=True, monitor=monitor, cache=cache)

    # Get FEI path
    feistate = fei.get_path(particles, configuration)

    # Add FEI path to the path to be processed
    path.add_path(feistate.path)

    for m in path.modules():
        if 'VariablesToNtuple' in m.name():
            newtreename = ''
            newfilename = 'training_input.root'
            oldfilename = ''
            for par in m.available_params():
                if par.name == 'fileName' and 'Monitor_Final' not in par.values:
                    newtreename = par.values.replace(' variables', '').replace('.root', '') + ' variables'
                    # print('\tReplacing', par.name, par.values, 'by', newfilename)
                    m.param('fileName', newfilename)
                elif par.name == 'fileName' and 'Monitor_Final' in par.values:
                    oldfilename = par.values
                    newfilename = 'Monitor_Final.root'
                    if oldfilename != newfilename:
                        newtreename = par.values.replace(' variables', '').replace('.root', '').replace('Monitor_Final_', '') + \
                                      ' variables'
                        # print('\tReplacing', par.name, par.values, 'by', newfilename)
                        m.param('fileName', newfilename)

            for par in m.available_params():
                if par.name == 'treeName' and ' ==> ' not in par.values and 'Monitor_Final' not in oldfilename:
                    # print('\tReplacing', par.name, par.values, 'by', newtreename)
                    m.param('treeName', newtreename)
                elif par.name == 'treeName' and ' ==> ' not in par.values and 'Monitor_Final' in oldfilename:
                    if oldfilename != newfilename:
                        # print('\tReplacing', par.name, par.values, 'by', newtreename)
                        m.param('treeName', newtreename)

    if verbose:
        for m in path.modules():
            if 'VariablesToNtuple' in m.name():
                print(m.name())
                for par in m.available_params():
                    if par.name in ['fileName', 'treeName']:
                        print(f'\t {par.name}: {par.values}')

    for m in path.modules():
        if 'VariablesToHistogram' in m.name():
            newfilename = ''
            oldfilename = ''
            newdirname = ''
            for par in m.available_params():
                if par.name == 'fileName' and ':' in par.values:
                    oldfilename = par.values
                    newdirname = par.values.replace('.root', '').replace('Monitor_', '')\
                                           .replace('PostReconstruction_', '').replace('PreReconstruction_', '')\
                                           .replace('AfterMVA_', '').replace('AfterVertex_', '')\
                                           .replace('AfterRanking_', '').replace('BeforeRanking_', '')\
                                           .replace('BeforePostCut_', '')
                    newfilename = par.values.replace('_' + newdirname, '')
                    # print('\tReplacing', par.name, par.values, 'by', newfilename)
                    m.param('fileName', newfilename)

            for par in m.available_params():
                if ':' in oldfilename:
                    # print('\tReplacing', par.name, par.values, 'by', newdirname)
                    m.param('directory', newdirname)

    if verbose:
        for m in path.modules():
            if 'VariablesToHistogram' in m.name():
                print(m.name())
                for par in m.available_params():
                    if par.name in ['fileName', 'directory', 'variables', 'variables_2d']:
                        print(f'\t {par.name}: {par.values}')

    # Add RootOutput to save particles reconstructing during the training stage
    # path.add_module('RootOutput') # disable, since not needed for training (only variable ntuples needed)
    return path


if __name__ == "__main__":
    # b2.process(create_fei_path(monitor=True))
    b2.process(create_fei_path(filelist=["/ceph/akhmet/test_fei_gbasf2_example_input/sub00/"
                                         "mdst_000001_prod00014140_task00000001.root"], monitor=True), max_event=10)
