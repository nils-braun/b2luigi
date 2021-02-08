from collections import namedtuple
from glob import glob
import enum

from b2luigi.core.utils import product_dict, fill_kwargs_with_lists

import b2luigi

from parse import parse


class DataMode(enum.Enum):
    raw = "raw"
    mdst = "mdst"
    cdst = "cdst"
    skimmed_raw = "skimmed_raw"


class DataTask(b2luigi.ExternalTask):
    data_mode = b2luigi.EnumParameter(enum=DataMode)
    experiment_number = b2luigi.IntParameter()
    run_number = b2luigi.IntParameter()
    prefix = b2luigi.Parameter()
    file_name = b2luigi.Parameter()


class RawDataTask(DataTask):
    data_mode = DataMode.raw

    def output(self):
        yield {"raw_output.root": b2luigi.LocalTarget(_build_data_path(self))}


class DstDataTask(DataTask):
    release = b2luigi.Parameter()
    prod = b2luigi.IntParameter()
    database = b2luigi.IntParameter()

    def output(self):
        yield {"full_output.root": b2luigi.LocalTarget(_build_data_path(self))}


class SkimmedRawDataTask(DstDataTask):
    data_mode = DataMode.skimmed_raw

    def output(self):
        yield {"raw_output.root": b2luigi.LocalTarget(_build_data_path(self))}


class MdstDataTask(DstDataTask):
    data_mode = DataMode.mdst

    def output(self):
        yield {"output.root": b2luigi.LocalTarget(_build_data_path(self))}


class CdstDataTask(DstDataTask):
    data_mode = DataMode.cdst

    def output(self):
        yield {"output.root": b2luigi.LocalTarget(_build_data_path(self))}


requires_raw_data = b2luigi.requires(RawDataTask)
requires_skimmed_raw_data = b2luigi.requires(SkimmedRawDataTask)
requires_mdst_data = b2luigi.requires(MdstDataTask)
requires_cdst_data = b2luigi.requires(CdstDataTask)


def _get_dir_structure(data_mode):
    if data_mode == DataMode.mdst:
        return b2luigi.get_setting("mdst_dir_structure",
                                   "/hsm/belle2/bdata/Data/release-{p.release}/DB{p.database:08d}/prod{p.prod:08d}/" + \
                                   "e{p.experiment_number:04d}/4S/r{p.run_number:05d}/all/mdst/sub00/" + \
                                   "mdst.{p.prefix}.{p.experiment_number:04d}.{p.run_number:05d}.{p.file_name}.root")
    if data_mode == DataMode.cdst:
        return b2luigi.get_setting("cdst_dir_structure",
                                   "/hsm/belle2/bdata/Data/release-{p.release}/DB{p.database:08d}/prod{p.prod:08d}/" + \
                                   "e{p.experiment_number:04d}/4S/r{p.run_number:05d}/all/cdst/sub00/" + \
                                   "cdst.{p.prefix}.{p.experiment_number:04d}.{p.run_number:05d}.{p.file_name}.root")
    if data_mode == DataMode.skimmed_raw:
        return b2luigi.get_setting("skimmed_raw_dir_structure",
                                   "/hsm/belle2/bdata/Data/release-{p.release}/DB{p.database:08d}/prod{p.prod:08d}/" + \
                                   "e{p.experiment_number:04d}/4S/r{p.run_number:05d}/all/raw/sub00/" + \
                                   "raw.{p.prefix}.{p.file_name}.{p.experiment_number:04d}.{p.run_number:05d}.root")
    if data_mode == DataMode.raw:
        return b2luigi.get_setting("raw_dir_structure",
                                   "/ghi/fs01/belle2/bdata/Data/Raw/e{p.experiment_number:04d}/r{p.run_number:05d}/sub00/" + \
                                   "{p.prefix}.{p.experiment_number:04d}.{p.run_number:05d}.{p.file_name}.root")


def _build_data_path(parameters):
    mode = parameters.data_mode

    dir_structure = _get_dir_structure(mode)
    return dir_structure.format(p=parameters)


def _parse_data_path(data_mode, data_path):
    extracted_kwargs = None

    dir_structure = _get_dir_structure(data_mode)
    extracted_kwargs = parse(dir_structure, data_path)

    if not extracted_kwargs:
        raise ValueError("The path does not fit the given format")
    extracted_kwargs = {key.replace("p.", ""): value for key, value in extracted_kwargs.named.items()}
    return extracted_kwargs


def _get_data_kwargs(data_mode, experiment_number, run_number, prefix=None, file_name=None, **other_kwargs):
    if file_name is None:
        file_name = "*"

    if prefix is None:
        prefix = "*"

    all_kwargs = fill_kwargs_with_lists(data_mode=data_mode, experiment_number=experiment_number, run_number=run_number,
                                        prefix=prefix,
                                        file_name=file_name, **other_kwargs)
    for kwargs in product_dict(**all_kwargs):
        # The build_data_path wants an object, so lets convert the dict to a named tuple
        kwargs = namedtuple('GenericDict', kwargs.keys())(**kwargs)
        for data_file in glob(_build_data_path(kwargs)):
            yield _parse_data_path(data_mode, data_file)


def clone_on_mdst(self, task_class, experiment_number, run_number, release, prod, database, prefix=None, file_name=None,
                  **additional_kwargs):
    # TODO: make database not needed
    for kwargs in _get_data_kwargs(data_mode=DataMode.mdst, experiment_number=experiment_number, run_number=run_number,
                                   release=release,
                                   prod=prod, database=database, prefix=prefix, file_name=file_name):
        yield self.clone(task_class, **kwargs, **additional_kwargs)


def clone_on_cdst(self, task_class, experiment_number, run_number, release, prod, database, prefix=None, file_name=None,
                  **additional_kwargs):
    # TODO: make database not needed
    for kwargs in _get_data_kwargs(data_mode=DataMode.cdst, experiment_number=experiment_number, run_number=run_number,
                                   release=release,
                                   prod=prod, database=database, prefix=prefix, file_name=file_name):
        yield self.clone(task_class, **kwargs, **additional_kwargs)


def clone_on_skimmed_raw(self, task_class, experiment_number, run_number, release, prod, database, prefix=None, file_name=None,
                  **additional_kwargs):
    # TODO: make database not needed
    for kwargs in _get_data_kwargs(data_mode=DataMode.skimmed_raw, experiment_number=experiment_number, run_number=run_number,
                                   release=release, 
                                   prod=prod, database=database, prefix=prefix, file_name=file_name):
        yield self.clone(task_class, **kwargs, **additional_kwargs)


def clone_on_raw(self, task_class, experiment_number, run_number, prefix=None, file_name=None, **additional_kwargs):
    for kwargs in _get_data_kwargs(data_mode=DataMode.raw, experiment_number=experiment_number, run_number=run_number,
                                   prefix=prefix,
                                   file_name=file_name):
        yield self.clone(task_class, **kwargs, **additional_kwargs)
