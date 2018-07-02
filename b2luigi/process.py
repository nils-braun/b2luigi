from . import utils
from . import core
from . import helper_tasks
from . import settings
from . import runner

import luigi
import basf2

import inspect
import os
import argparse


def process_luigi_tasks(task_list, kwargs):
    if os.environ.get("B2LUIGI_EXECUTION", False):
        return core.run_task_from_env()
    
    scheduler_url = kwargs.pop("scheduler_url")
    local = kwargs.pop("local")

    print(kwargs, task_list)

    if scheduler_url:
        runner.schedule(task_list, scheduler_url=scheduler_url, **kwargs)
    elif local:
        runner.schedule(task_list, local_scheduler=True, **kwargs)
    else:
        socket_address = kwargs.pop("socket_address", None)

        if not socket_address:
            socket_address = runner.run_luigid()

        runner.run_batch_jobs(socket_address)

def show_all_outputs(task_list):
    pass


def get_tasks_from_path_creator_function(path_creator_function, kwargs):
    parameters = inspect.signature(path_creator_function).parameters

    for key, param in parameters.items():
        setattr(helper_tasks.Basf2PathCreatorTask, key, core.NonParseableParameter())

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


__has_run_already = False
        
def process(arg, **kwargs):
    # Assert, that process is only run once
    global __has_run_already
    if __has_run_already:
        raise RuntimeError("You are not allowed to call process twice in your code!")
    __has_run_already = True

    # Create Arguments Parser
    parser = argparse.ArgumentParser()

    parser.add_argument("--show-output", help="Instead of running the tasks, show which output files will/are created.", action="store_true")
    parser.add_argument("--local", action="store_true")
    parser.add_argument("--scheduler-url", default="")

    args = parser.parse_args()

    # Create Task List
    if isinstance(arg, basf2.Path):
        task_list = get_tasks_from_basf2_path(arg, kwargs)
    elif callable(arg):
        task_list = get_tasks_from_path_creator_function(arg, kwargs)
    else:
        task_list = arg

    # Go
    if args.show_output:
        show_all_outputs(task_list)
    else:
        kwargs.setdefault("local", args.local)
        kwargs.setdefault("scheduler_url", args.scheduler_url)

        process_luigi_tasks(task_list, kwargs)

    
