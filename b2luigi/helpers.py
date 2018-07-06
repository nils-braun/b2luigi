from collections import namedtuple
from glob import glob
import enum

from b2luigi.core.utils import product_dict, fill_kwargs_with_lists

import b2luigi

from parse import parse


MDST_DIR_STRUCTURE = "/hsm/belle2/bdata/Data/release-{p.release}/DB{p.database:08d}/prod{p.prod:08d}/" + \
                     "e{p.experiment:04d}/4S/r{p.run:05d}/all/mdst/sub00/" + \
                     "mdst.{p.prefix}.{p.experiment:04d}.{p.run:05d}.{p.file_name}.root"
CDST_DIR_STRUCTURE = "/hsm/belle2/bdata/Data/release-{p.release}/DB{p.database:08d}/prod{p.prod:08d}/" + \
                     "e{p.experiment:04d}/4S/r{p.run:05d}/all/cdst/sub00/" + \
                     "cdst.{p.prefix}.{p.experiment:04d}.{p.run:05d}.{p.file_name}.root"
RAW_DIR_STRUCTURE = "/ghi/fs01/belle2/bdata/Data/Raw/e{p.experiment:04d}/r{p.run:05d}/sub00/" + \
                    "{p.prefix}.{p.experiment:04d}.{p.run:05d}.{p.file_name}.root"


class DataMode(enum.Enum):
    raw = "raw"
    mdst = "mdst"
    cdst = "cdst"


class DataTask(b2luigi.ExternalTask):
    data_mode = b2luigi.EnumParameter(enum=DataMode)
    experiment = b2luigi.IntParameter()
    run = b2luigi.IntParameter()
    prefix = b2luigi.Parameter()
    file_name = b2luigi.Parameter()

    def output(self):
        yield {"input.root": b2luigi.LocalTarget(_build_data_path(self))}


class RawDataTask(DataTask):
    data_mode = DataMode.raw


class DstDataTask(DataTask):
    release = b2luigi.Parameter()
    prod = b2luigi.IntParameter()
    database = b2luigi.IntParameter()


class MdstDataTask(DstDataTask):
    data_mode = DataMode.mdst


class CdstDataTask(DstDataTask):
    data_mode = DataMode.cdst



def _build_data_path(parameters):
    mode = parameters.data_mode

    if mode == DataMode.raw:
        return RAW_DIR_STRUCTURE.format(p=parameters)
    elif mode == DataMode.cdst:
        return CDST_DIR_STRUCTURE.format(p=parameters)
    elif mode == DataMode.mdst:
        return MDST_DIR_STRUCTURE.format(p=parameters)


def _parse_data_path(data_mode, data_path):
    extracted_kwargs = None
    if data_mode == DataMode.mdst:
        extracted_kwargs = parse(MDST_DIR_STRUCTURE, data_path)
    elif data_mode == DataMode.cdst:
        extracted_kwargs = parse(CDST_DIR_STRUCTURE, data_path)
    elif data_mode == DataMode.raw:
        extracted_kwargs = parse(RAW_DIR_STRUCTURE, data_path)

    if not extracted_kwargs:
        raise ValueError("The path does not fit the given format")
    extracted_kwargs = {key.replace("p.", ""): value for key, value in extracted_kwargs.named.items()}
    return extracted_kwargs


def _get_data_kwargs(data_mode, experiment, run, prefix=None, file_name=None, **other_kwargs):
    if file_name is None:
        file_name = "*"

    if prefix is None:
        prefix = "*"

    all_kwargs = fill_kwargs_with_lists(data_mode=data_mode, experiment=experiment, run=run, prefix=prefix,
                                        file_name=file_name, **other_kwargs)

    for kwargs in product_dict(**all_kwargs):
        # The build_data_path wants an object, so lets convert the dict to a named tuple
        kwargs = namedtuple('GenericDict', kwargs.keys())(**kwargs)
        for data_file in glob(_build_data_path(kwargs)):
            yield _parse_data_path(data_mode, data_file)


class TaskWithDataSupport:
    def clone_on_mdst(self, task_class, experiment, run, release, prod, database, prefix=None, file_name=None,
                      **additional_kwargs):
        # TODO: make database not needed
        for kwargs in _get_data_kwargs(data_mode=DataMode.mdst, experiment=experiment, run=run, release=release,
                                       prod=prod, database=database, prefix=prefix, file_name=file_name):
            yield self.clone(task_class, **kwargs, **additional_kwargs)

    def clone_on_cdst(self, task_class, experiment, run, release, prod, database, prefix=None, file_name=None,
                      **additional_kwargs):
        # TODO: make database not needed
        for kwargs in _get_data_kwargs(data_mode=DataMode.cdst, experiment=experiment, run=run, release=release,
                                       prod=prod, database=database, prefix=prefix, file_name=file_name):
            yield self.clone(task_class, **kwargs, **additional_kwargs)

    def clone_on_raw(self, task_class, experiment, run, prefix=None, file_name=None, **additional_kwargs):
        for kwargs in _get_data_kwargs(data_mode=DataMode.raw, experiment=experiment, run=run, prefix=prefix,
                                       file_name=file_name):
            yield self.clone(task_class, **kwargs, **additional_kwargs)
