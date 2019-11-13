import json
import os
import contextlib
import warnings

# The global object hosting the current settings
_current_global_settings = {}


def get_setting(key, default=None, task=None, deprecated_keys=None):
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
        deprecated_keys (:obj:`List`): Former names of this setting,
            will throw a warning when still used
    """
    # First check if the correct name is set. If yes, just use it
    try:
        return _get_setting_implementation(key=key, task=task)
    except KeyError:
        pass

    # Ok, now test the deprecated setting names, but issue a warning
    if deprecated_keys:
        for deprecated_key in deprecated_keys:
            try:
                value = _get_setting_implementation(key=deprecated_key, task=task)
                _warn_deprecated_setting(deprecated_key, key)
                return value
            except KeyError:
                pass 

    # Still not found? At this point we can only return the default or raise an error
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


def _get_setting_implementation(key, task):
    """
    Implementation of the settings retrieval.
    Either get it from the task,
    or from the user-defined settings
    or from the setting files.
    If nothing is set, raise a KeyError.
    """
    # First check if the task has an attribute with this name
    if task:
        try:
            return getattr(task, key)
        except AttributeError:
            pass

    # Then check if the setting was set explicitely
    try:
        return _current_global_settings[key]
    except KeyError:
        pass

    # And finally check the settings files
    for settings_file in _setting_file_iterator():
        try:
            with open(settings_file, "r") as f:
                j = json.load(f)
                return j[key]
        except KeyError:
            pass

    # The setting was not found, so raise a KeyError
    raise KeyError(f"No settings found for {key}!")


class DeprecatedSettingsWarning(DeprecationWarning):
    pass

def _warn_deprecated_setting(setting_name, new_name):
    warnings.warn(f"The setting with the name {setting_name} is deprecated. "
                  f"Please use {new_name} instead. Future versions might remove this setting.",
                  DeprecatedSettingsWarning)