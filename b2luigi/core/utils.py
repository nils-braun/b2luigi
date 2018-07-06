import git

import base64
import itertools
import pickle
import os
import collections


PREFIX = "B2LUIGI_PARAM_"


def product_dict(**kwargs):
    keys = kwargs.keys()
    vals = kwargs.values()
    for instance in itertools.product(*vals):
        yield dict(zip(keys, instance))


def fill_kwargs_with_lists(**kwargs):
    return_kwargs = {}
    for key, value in kwargs.items():
        if value is None:
            value = []
        if not isinstance(value, list):
            value = [value]
        return_kwargs[key] = value

    return return_kwargs


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

    return {key: value.path for key, value in inputs.items()}

def flatten_to_dict(inputs):
    if isinstance(inputs, dict):
        return inputs

    if isinstance(inputs, collections.Iterable):
        joined_dict = {}
        for i in inputs:
            joined_dict.update(**i)
        return joined_dict

    return {inputs: inputs}


def task_iterator(task):
    yield task
    for dep in task.deps():
        yield from task_iterator(dep)