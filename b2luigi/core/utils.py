import git

import base64
import itertools
import pickle
import os
import collections

import luigi

PREFIX = "B2LUIGI_PARAM_"


def product_dict(**kwargs):
    keys = kwargs.keys()
    vals = kwargs.values()
    for instance in itertools.product(*vals):
        yield dict(zip(keys, instance))


def encode_value(value):
    pickled_value = pickle.dumps(value)
    encoded_bytes = base64.b64encode(pickled_value)
    encoded_string = encoded_bytes.decode()
    return encoded_string


def decode_value(encoded_string):
    encoded_bytes = encoded_string.encode()
    pickled_value = base64.b64decode(encoded_bytes)
    value = pickle.loads(pickled_value)
    return value


def get_basf2_git_hash():
    basf2_release = os.getenv("BELLE2_RELEASE")

    assert basf2_release

    if basf2_release == "head":
        basf2_release_location = os.getenv("BELLE2_LOCAL_DIR")

        assert basf2_release_location
        return git.Repo(basf2_release_location).head.object.hexsha

    return basf2_release


def flatten_to_file_paths(inputs):
    if not inputs:
        return None

    if isinstance(inputs, dict):
        return {key: flatten_to_file_paths(i) for key, i in inputs.items()}

    if isinstance(inputs, collections.Iterable):
        return luigi.task.flatten([flatten_to_file_paths(i) for i in inputs])

    return inputs.path


def task_iterator(task):
    yield task
    for dep in task.deps():
        yield from task_iterator(dep)