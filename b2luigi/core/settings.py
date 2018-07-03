import json
import os


# The global object hosting the current settings
_current_global_settings = {}


def get_setting(key, default=None):
    try:
        return _current_global_settings[key]
    except KeyError:
        for settings_file in _setting_file_iterator():
            try:
                with open(settings_file , "r") as f:
                    return json.load(f)[key]
            except KeyError:
                pass

    if default is None:
        raise ValueError(f"No settings found for {key}!")
    return default


def set_setting(key, value):
    _current_global_settings[key] = value


def _setting_file_iterator():
    path = os.getcwd()

    while True:
        json_file = os.path.join(path, "settings.json")
        if os.path.exists(json_file):
            yield json_file

        path = os.path.split(path)[0]
        if path == "/":
            break
