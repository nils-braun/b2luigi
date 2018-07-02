import json
import os


class SettingsObject:
    _current_global_settings = {}

    @staticmethod
    def get_setting(key, default=None):
        try:
            return SettingsObject._current_global_settings[key]
        except KeyError:
            path = os.getcwd()

            while True:
                json_file = os.path.join(path, "settings.json")
                if os.path.exists(json_file):
                    try:
                        with open(json_file, "r") as f:
                            return json.load(f)[key]
                    except KeyError:
                        pass

                path = os.path.split(path)[0]
                if path == "/":
                    break

        if default is None:
            raise ValueError(f"No settings found for {key}!")
        return default

    @staticmethod
    def set_setting(key, value):
        SettingsObject._current_global_settings[key] = value


set_settings = SettingsObject.set_setting
get_settings = SettingsObject.get_setting