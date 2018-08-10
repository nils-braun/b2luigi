import hashlib
import luigi

from luigi.parameter import _no_value


class BoolParameter(luigi.BoolParameter):
    """Copied BoolParameter without default value"""
    def __init__(self, **kwargs):
        kwargs.setdefault("default", _no_value)
        luigi.Parameter.__init__(self, **kwargs)
