# -*- coding: utf-8 -*-

import os, tempfile

from jsondb.backends.sqlite3_backend import Sqlite3Backend
from jsondb.backends.url import URL

drivers = {
    'sqlite3' : Sqlite3Backend,
}

class Error(Exception):
    pass

class NonAvailableError(Error):
    pass

def create(name, *args, **kws):
    cls = drivers.get(name.lower(), None)
    if not cls:
        raise NonAvailableError, name
    return cls(*args, **kws) if cls else None


def create(connstr, *args, **kws):
    if not connstr:
        # assume sqlite3
        fd, path = tempfile.mkstemp(suffix='.jsondb')
        connstr = 'sqlite3://%s' % (os.path.abspath(os.path.normpath(path)))

    try:
        url = URL.parse(connstr)
    except:
        path = connstr
        connstr = 'sqlite3:///%s' % (os.path.abspath(os.path.normpath(path)))
        url = URL.parse(connstr)

    if not url.driver:
        url.driver = 'sqlite3'

    name = url.driver
    cls = drivers.get(name.lower(), None)
    if not cls:
        raise NonAvailableError, name
    return cls(url=url, *args, **kws) if cls else None

