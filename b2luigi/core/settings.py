import json
import os
import contextlib

# The global object hosting the current settings
_current_global_settings = {}


def get_setting(key, default=None):
    """
    ``b2luigi`` adds a settings management to ``luigi`` 
    and also uses it at various places.

    With this function, you can get the current value of
    a specific setting with the given key.
    If there is no setting defined with this name,
    either the default is returned or, if you did not 
    supply any default, a value error is raised.

    For information on how settings are set, please 
    see :obj:`set_setting`.
    Settings can be of any type, but are mostly strings.

    Parameters:
        key (:obj:`str`): The name of the parameter to query.
        default (optional): If there is no setting which the name, 
            either return this default or if it is not set, 
            raise a ValueError.
    """
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


def get_task_setting(key, default=None, task=None):
    """
    Return the setting value of `key` 
    * either if it is set as an attribute of the specified task
    * or if not (or no task is given) as set by the setting files 
      or current global setting
    * or if nothing is specified, returns the default (or raise 
      an exception of no default is given)
    """
    if task:
        try:
            return getattr(task, key)
        except AttributeError:
            pass

    return get_setting(key, default)


def set_setting(key, value):
    """
    There are two possibilities to set a setting with a given name:

    * Either you use this function and supply the key and the value.
      The setting is then defined globally for all following calls
      to :obj:`get_setting` with the specific key.
    * Or you add a file called ``settings.json`` the the current
      working directory *or any folder above that*.
      In the json file, you need to supply a key and a value for each
      setting you want to have, e.g::

        {
            "result_path": "results",
            "some_setting": "some_value"
        }

      By looking also in the parent folders for setting files, you can
      define project settings in a top folder and specific settings
      further down in your local folders.

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
