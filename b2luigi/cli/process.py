from b2luigi.cli.arguments import get_cli_arguments
from b2luigi.cli.runner import run_local, run_test_mode
from b2luigi.core import tasks, helper_tasks, utils

import basf2

import inspect
import os


############################################################################
# TODO: remove all this
def show_all_outputs(task_list):
    pass


def get_tasks_from_path_creator_function(path_creator_function, kwargs):
    parameters = inspect.signature(path_creator_function).parameters

    for key, param in parameters.items():
        setattr(helper_tasks.Basf2PathCreatorTask, key, tasks.NonParseableParameter())

    refined_kwargs = {}
    for key, param in parameters.items():
        if param.default == inspect.Parameter.empty:
            value = kwargs.pop(key)
        else:
            value = kwargs.pop(key, param.default)

        refined_kwargs[key] = value

    product_dict = utils.product_dict(**refined_kwargs)

    return [helper_tasks.Basf2PathCreatorTask(path_creator_function=path_creator_function, **path_kwargs) for path_kwargs in product_dict]


global_basf2_path = None


def create_path():
    return global_basf2_path


def get_tasks_from_basf2_path(basf2_path, kwargs):
    max_event = kwargs.pop("max_event", 0)

    helper_tasks.Basf2PathTask.basf2_path = basf2_path
    return [helper_tasks.Basf2PathTask(max_event=max_event)]
############################################################################


__has_run_already = False


def process(task_like_elements, **kwargs):
    # Assert, that process is only run once
    global __has_run_already
    if __has_run_already:
        raise RuntimeError("You are not allowed to call process twice in your code!")
    __has_run_already = True

    # Create Task List
    if isinstance(task_like_elements, basf2.Path):
        task_list = get_tasks_from_basf2_path(task_like_elements, kwargs)
    elif callable(task_like_elements):
        task_list = get_tasks_from_path_creator_function(task_like_elements, kwargs)
    elif not isinstance(task_like_elements, list):
        task_list = [task_like_elements]
    else:
        task_list = task_like_elements

    # Run now if requested
    if os.environ.get("B2LUIGI_EXECUTION", False):
        return tasks.run_task_from_env()

    # Check the CLI arguments and run as requested
    cli_args = get_cli_arguments()

    if cli_args.show_output:
        show_all_outputs(task_list)
    elif cli_args.test:
        run_test_mode(task_list, cli_args, kwargs)
    else:
        run_local(task_list, cli_args, kwargs)



