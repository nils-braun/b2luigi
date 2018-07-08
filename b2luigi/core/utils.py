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


def get_all_output_files_in_tree(root_module, key=None):
    if key:
        return get_all_output_files_in_tree(root_module)[key]

    all_output_files = collections.defaultdict(list)
    for task in task_iterator(root_module):
        output_dict = task.get_output_file_names()
        if not output_dict:
            continue

        for file_key, file_name in output_dict.items():
            all_output_files[file_key].append(dict(parameters=task.get_filled_params(),
                                                   file_name=os.path.abspath(file_name)))

    return all_output_files


def filter_from_params(output_files, **kwargs):
    kwargs_list = fill_kwargs_with_lists(**kwargs)

    if not kwargs_list:
        return output_files

    file_names = set()

    for kwargs in product_dict(**kwargs_list):
        for output_dict in output_files:
            parameters = output_dict["parameters"]
            file_name = output_dict["file_name"]

            not_use = False
            for key, value in kwargs.items():
                if key in parameters and parameters[key] != value:
                    not_use = True
                    break

            if not_use:
                continue

            file_names.add(file_name)

    return list(file_names)
