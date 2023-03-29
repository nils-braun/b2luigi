import hashlib

import luigi
from luigi.parameter import _no_value
from inspect import signature


def wrap_parameter():
    """
    Monkey patch the parameter base class (and with it all other parameters(
    of luigi to include two additional parameters in its constructor:
    "hashed" and "hash_function".

    Enabling the "hashed" parameter will use a hashed version of the
    parameter value when creating file paths our of the parameters of a task
    instead of the value itself. By default an md5 hash is used. A custom
    hash function can be provided via the "hash_function" parameter. This
    function should take one input, the value of the parameter. It is up
    to the user to ensure a unique string is created from the input.

    This is especially useful when you have list, string or dict parameters,
    where the resulting file path may include "/" or "{}".
    """
    import b2luigi
    parameter_class = b2luigi.Parameter

    def serialize_hashed(self, x):
        if self.hash_function is None:
            return "hashed_" + hashlib.md5(str(x).encode()).hexdigest()
        else:
            return self.hash_function(x)

    old_init = parameter_class.__init__

    def __init__(self, hashed=False, hash_function=None, *args, **kwargs):
        old_init(self, *args, **kwargs)

        if self.hash_function is not None:
            n_params = len(signature(hash_function).params)
            assert  n_params == 1, f"Custom hash function can have only"\
                " 1 argument, found {n_params}"

        self.hash_function = hash_function

        if hashed:
            self.serialize_hashed = lambda x: serialize_hashed(self, x)

    parameter_class.__init__ = __init__


class BoolParameter(luigi.BoolParameter):
    """Copied BoolParameter without default value"""
    def __init__(self, **kwargs):
        kwargs.setdefault("default", _no_value)
        luigi.Parameter.__init__(self, **kwargs)
