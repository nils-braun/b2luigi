import json
import os
import contextlib

# The global object hosting the current settings
_current_global_settings = {}


def get_setting(key, default=None, task=None):
    """
    ``b2luigi`` adds a settings management to ``luigi`` 
    and also uses it at various places.
    Many batch systems, the output and log path, the environment
    etc. is controlled via these settings.

    There are four ways settings could be defined.
    They are used in the following order (an earlier setting
    overrides a later one):

    1. If the currently processed (or scheduled) task has a property
       of the given name, it is used.
       Please note that you can either set the property directly, e.g.

       .. code-block:: python
 
         class MyTask(b2luigi.Task):
             batch_system = "htcondor"

       or by using a function (which might even depend on the parameters)
 
       .. code-block:: python
 
         class MyTask(b2luigi.Task):
             @property
             def batch_system(self):
                 return "htcondor"

       The latter is especially useful for batch system specific settings 
       such as requested wall time etc.

    2. Settings set directly by the user in your script with a call to 
       :meth:`b2luigi.set_setting`.
    3. Settings specified in the ``settings.json`` in the folder of your 
       script *or any folder above that*.
       This makes it possible to have general project settings (e.g. the output path 
       or the batch system) and a specific ``settings.json`` for your sub-project.

    With this function, you can get the current value of a specific setting with the given key.
    If there is no setting defined with this name,
    either the default is returned or, if you did not supply any default, a value error is raised.

    Settings can be of any type, but are mostly strings.

    Parameters:
        key (:obj:`str`): The name of the parameter to query.
        task: (:obj:`b2luigi.Task`): If given, check if the task has a parameter 
            with this name.
        default (optional): If there is no setting which the name, 
            either return this default or if it is not set, 
            raise a ValueError.
    """
    if task:
        try:
            return getattr(task, key)
        except AttributeError:
            pass

    try:
        return _current_global_settings[key]
    except KeyError:
        for settings_file in _setting_file_iterator():
            try:
                with open(settings_file, "r") as f:
                    j = json.load(f)
                    return j[key]
            except KeyError:
                pass

    if default is None:
        raise ValueError(f"No settings found for {key}!")
    return default


def set_setting(key, value):
    """
    Set the setting with the specified name - overriding any ``setting.json``.
    If you want to have task specific settings, create a
    parameter with the given name or your task.
    """
    _current_global_settings[key] = value


def clear_setting(key):
    """
    Clear the setting with the given key
    """
    try:
        del _current_global_settings[key]
    except KeyError:
        pass


def _setting_file_iterator():
    path = os.getcwd()

    while True:
        json_file = os.path.join(path, "settings.json")
        if os.path.exists(json_file):
            yield json_file

        path = os.path.split(path)[0]
        if path == "/":
            break


@contextlib.contextmanager
def with_new_settings():
    global _current_global_settings
    old_settings = _current_global_settings.copy()
    _current_global_settings = {}

    yield

    _current_global_settings = old_settings.copy()
