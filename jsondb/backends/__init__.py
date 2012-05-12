# -*- coding: utf-8 -*-

from jsondb.backends.sqlite3_backend import Sqlite3Backend

mapping = {
    'sqlite3' : Sqlite3Backend,
}

class Error(Exception):
    pass

class NonAvailableError(Error):
    pass

def create(name, *args, **kws):
    cls = mapping.get(name.lower(), None)
    if not cls:
        raise NonAvailableError, name
    return cls(*args, **kws) if cls else None

