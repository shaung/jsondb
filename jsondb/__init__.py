# -*- coding: utf-8 -*-

try:
    import pkg_resources  # part of setuptools
    version = pkg_resources.require("jsondb")[0].version
except:
    version = 'unknown'

try:
    import simplejson as json
except:
    import json

import core
from datatypes import *
import backends
from error import *


class BaseDB:
    def set_link_key(self, link_key):
        self.link_key = link_key
        self.backend.set_link_key(link_key)

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        try:
            if traceback:
                self.backend.rollback()
            else:
                self.backend.commit()
            self.close()
        except:
            raise

    def dumprows(self):
        for row in self.backend.dumprows():
            print row


def get_class(datatype):
    cls = core.get_type_class(datatype)
    class JsonDB(cls, BaseDB):
        pass

    return JsonDB


def create(data={}, url=None, overwrite=True, link_key=None, **kws):
    """
    Create a new empty DB.

    :param data: Initial data. An empty dict if not specified.

    :param url: An RFC-1738-style string which specifies the URL to store into.

    :param overwrite: If is True and the database specified by *url* already exists, it will be truncated.

    :param link_key: Key directive for links in the database.

    :param kws: Additional parameters to parse to the engine.
    """
    _backend = backends.create(url, overwrite=overwrite)

    # guess root type from the data provided.
    root_type = TYPE_MAP.get(type(data))
    if root_type in (BOOL, INT):
        root = int(data)
    elif root_type == FLOAT:
        root = float(data)
    elif root_type in (DICT, LIST):
        root = ''
    else:
        root = data

    cls = get_class(root_type)
    self = cls(datatype=root_type, backend=_backend, link_key=link_key)

    self.backend.insert_root((root_type, root))

    if root_type == DICT:
        for k, v in data.iteritems():
            self.feed({k: v})
    elif root_type == LIST:
        for x in data:
            self.feed(x)

    self.commit()

    return self


def load(url, **kws):
    """
    Load from an existing DB.

    :param url: An RFC-1738-style string which specifies the URL to load from.

    :param kws: Additional parameters to parse to the engine.
    """
    _backend = backends.create(url, overwrite=False)
    root_type = _backend.get_root_type()

    cls = get_class(root_type)
    self = cls(backend=_backend)

    return self


def from_file(file, url=None, **kws):
    """Create a new db from json file"""
    if isinstance(file, basestring):
        fileobj = open(file)
    else:
        fileobj = file
    # TODO: streaming
    data = json.load(fileobj)

    self = create(url=url, **kws)
    try:
        self.feed(data)
    except:
        self.backend.rollback()
    else:
        self.commit()
    return self


__all__ = ['version', 'create', 'load', 'from_file']
