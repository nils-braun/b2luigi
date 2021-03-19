import pickle

from basf2 import conditions as b2conditions
from basf2.pickle_path import serialize_path
from variables import variables as vm


def get_alias_dict_from_variable_manager():
    """
    Extracts a dictionary with the alias names as keys and their values from the
    internal state of the variable manager and returns it.
    """
    alias_dictionary = {alias_name: vm.getVariable(alias_name).name for alias_name in list(vm.getAliasNames())}
    return alias_dictionary


def write_path_and_state_to_file(basf2_path, file_path):
    """
    Serialize basf2 path and variables from variable manage to file.

    Variant of ``basf2.pickle_path.write_path_to_file``, only with additional
    serialization of the basf2 variable aliases and global tags.

    The aliases are extracted from the current state of the variable manager singleton
    and thus have to be added in the python/basf2 process before calling this function.
    Likewise for the global tags.

    :param path: Basf2 path object to serialize
    :param file_path: File path to write the serialized pickle object to.
    """
    with open(file_path, 'bw') as pickle_file:
        serialized = serialize_path(basf2_path)
        serialized["aliases"] = get_alias_dict_from_variable_manager()
        serialized["globaltags"] = b2conditions.globaltags
        # serialized["conditions"] = b2conditions
        pickle.dump(serialized, pickle_file)
