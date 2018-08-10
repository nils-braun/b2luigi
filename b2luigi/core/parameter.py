import hashlib

import luigi
from luigi.parameter import _no_value


def wrap_parameter():
    """
    Monkey patch the parameter base class (and with it all other parameters(
    of luigi to include an additional "hashed" parameter in its constructor.

    Enabling this parameter will use a hashed version of the parameter value
    when creating file paths our of the parameters of a task instead of the
    value itself.
    This is especially useful when you have list, string or dict parameters,
    where the resulting file path may include "/" or "{}".
    """
    import b2luigi
    parameter_class = b2luigi.Parameter

    def serialize_hashed(self, x):
        return "hashed_" + hashlib.md5(str(x).encode()).hexdigest()

    old_init = parameter_class.__init__

    def __init__(self, hashed=False, *args,  **kwargs):
        old_init(self, *args, **kwargs)

        if hashed:
            self.serialize_hashed = lambda x: serialize_hashed(self, x)

    parameter_class.__init__ = __init__


class BoolParameter(luigi.BoolParameter):
    """Copied BoolParameter without default value"""
    def __init__(self, **kwargs):
        kwargs.setdefault("default", _no_value)
        luigi.Parameter.__init__(self, **kwargs)
