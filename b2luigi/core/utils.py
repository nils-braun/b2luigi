import contextlib
import importlib

import base64
import itertools
import pickle
import os
import collections

from b2luigi.core.settings import set_setting

@contextlib.contextmanager
def remember_cwd():
    old_cwd = os.getcwd()
    try:
        yield
    finally:
        os.chdir(old_cwd)


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
            all_output_files[file_key].append(dict(parameters=task.get_serialized_parameters(),
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

            not_use = False
            for key, value in kwargs.items():
                if key in parameters and parameters[key] != value:
                    not_use = True
                    break

            if not_use:
                continue

            file_names.add(output_dict)

    return list(file_names)


def get_task_from_file(file_name, task_name, settings=None, **kwargs):
    with remember_cwd():
        os.chdir(os.path.dirname(file_name))
        spec = importlib.util.spec_from_file_location("module.name", os.path.basename(file_name))
        task_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(task_module)

        if settings:
            for key, value in settings.items():
                set_setting(key, value)
        m = getattr(task_module, task_name)(**kwargs)

        return m