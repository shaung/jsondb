# coding: utf8


class Cache(object):
    def __init__(self):
        self.data = {}

    def __setitem__(self, key, value):
        self.data[key] = value

    def __getitem__(self, key):
        return self.data.get(key)

    def clear(self):
        self.data.clear()

_cache = Cache()


def set(key, value):
    _cache[key] = value


def get(key, default=None):
    result = _cache[key]
    return result if result is not None else default
