import hashlib

import b2luigi


class HashedParameter(b2luigi.Parameter):
    def serialize(self, x):
        return hashlib.md5(str(x).encode()).hexdigest()

    def parse(self, x):
        raise AttributeError("A hashed parameter can not be parsed")