import hashlib

import b2luigi


class HashedParameter(b2luigi.Parameter):
    def serialize_hashed(self, x):
        return "hashed_" + hashlib.md5(str(x).encode()).hexdigest()