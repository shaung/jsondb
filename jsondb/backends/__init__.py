# -*- coding: utf-8 -*-

from jsondb.backend.sqlite3_backend import Sqlite3Backend

mapping = {
    'sqlite3' : Sqlite3Backend,
}

def create(name, *args, **kws):
    cls = mapping.get(name.lower(), None)
    return cls(*args, **kws) if cls else None

