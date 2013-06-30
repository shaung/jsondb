# -*- coding: utf-8 -*-

import os
import tempfile
import re

from jsondb.backends.sqlite3_backend import Sqlite3Backend
from jsondb.backends.url import URL
from jsondb.util import IS_WINDOWS


drivers = {
    'sqlite3' : Sqlite3Backend,
}


class Error(Exception):
    pass


class NonAvailableSchemeError(Error):
    pass


def get_backend_class(name):
    cls = drivers.get(name)
    if not cls:
        try:
            name_lowered = name.lower()
            modname = '%s_backend' % name_lowered
            extbase = __import__('jsondb_backend', fromlist=[modname])
            mod = getattr(extbase, modname)
            clsname = '%sBackend' % name.capitalize()
            cls = getattr(mod, clsname)
        except:
            raise NonAvailableSchemeError

    return cls


def create(connstr, *args, **kws):
    if not connstr:
        # assume sqlite3
        fd, path = tempfile.mkstemp(suffix='.jsondb')
        connstr = 'sqlite3://%s' % (os.path.abspath(os.path.normpath(path)))
        if IS_WINDOWS:
            connstr = 'sqlite3:///%s' % (os.path.abspath(os.path.normpath(path)))

    if IS_WINDOWS and not re.match(r'^[^:/]+://.*$', connstr):
        connstr = 'sqlite3:///%s' % (os.path.abspath(os.path.normpath(connstr)))

    url = URL.parse(connstr)

    if not url.driver:
        url.driver = 'sqlite3'

    name = url.driver
    cls = get_backend_class(name.lower())
    if not cls:
        raise NonAvailableSchemeError(name)
    return cls(url=url, *args, **kws) if cls else None
