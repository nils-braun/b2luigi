
import collections
from glob import glob
import os


def recorded_data(experiment=None, run=None, pattern="*.root", basedir="."):
    return list(_recorded_data(experiment=experiment, run=run, pattern=pattern, basedir=basedir))


def _recorded_data(experiment, run, pattern, basedir):
    if isinstance(experiment, collections.Iterable):
        for single_experiment in experiment:
            yield from recorded_data(experiment=single_experiment,
                                     run=run, pattern=pattern, basedir=basedir)

        return

    if isinstance(run, collections.Iterable):
        for single_run in run:
            yield from recorded_data(experiment=experiment,
                                     run=single_run, pattern=pattern, basedir=basedir)

        return

    search_pattern = build_recorded_data_path(basedir, experiment, run, pattern)
    for file_name in glob(search_pattern):
        yield dict(
            experiment_number=experiment,
            run_number=run,
            basedir=basedir,
            filename=os.path.basename(file_name)
        )


def build_recorded_data_path(basedir, experiment, run, file_name):
    if experiment:
        experiment_string = f"e{experiment:04}"
    else:
        experiment_string = "*"
    if run:
        run_string = f"r{run:05}"
    else:
        run_string = "*"
    search_pattern = os.path.join(
        basedir, experiment_string, run_string, "sub00", file_name)
    return search_pattern
