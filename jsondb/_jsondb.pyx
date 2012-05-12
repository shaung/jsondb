# -*- coding: utf-8 -*-
# cython: profile=True

"""
    jsondb
    ~~~~~~

    Turn the json data into a database.
    Then query the data using JSONPath.
"""

import os
import tempfile
import re

try:
    import simplejson as json
except:
    import json

from datatypes import *
import backends

__version__  = '0.1'


class Error(Exception):
    pass


class UnsupportedTypeError(Error):
    pass


class IllegalTypeError(Error):
    pass


class QueryResult(object):
    def __init__(self, seq):
        self.seq = seq

    def getone(self):
        return next(self.seq)

    def values(self):
        for row in self.seq:
            yield row.value

    def __iter__(self):
        for row in self.seq:
            yield row


cdef class JsonDB:
    cdef public backend
    cdef public link_key

    def __init__(self,  backend, link_key=None):
        self.backend = backend
        self.link_key = link_key or '@__link__'

    @classmethod
    def create(cls, path=None, root_type=DICT, value=None, overwrite=True, link_key=None, backend_name='sqlite3'):
        """Create a new empty DB."""
        if not path:
            fd, path = tempfile.mkstemp(suffix='.jsondb')
        dbpath = os.path.normpath(path)
        _backend = backends.create(backend_name, filepath=dbpath, overwrite=overwrite)
        self = cls(backend=_backend, link_key=link_key)

        # TODO: guess root type from the value provided.
        if root_type in (BOOL, INT):
            value = int(value)
        elif root_type == FLOAT:
            value = float(value)

        self.backend.insert_root((root_type, value))
        self.commit()

        return self

    @classmethod
    def load(cls, path, **kws):
        """Load from an existing DB."""
        # TODO: path -> connstr
        _backend = backends.create('sqlite3', filepath=path, overwrite=False)
        self = cls(backend=_backend, link_key='')
        return self

    @classmethod
    def from_file(cls, dbpath, filepath, **kws):
        """Create a new db from json file"""
        if isinstance(filepath, basestring):
            fileobj = open(filepath)
        else:
            fileobj = filepath
        data = json.load(fileobj)

        _type = TYPE_MAP.get(type(data))
        self = cls.create(dbpath, root_type=_type, **kws)
        with self.backend.get_connection():
            self.feed(data)
            self.commit()
        return self

    def set_link_key(self, link_key):
        self.link_key = link_key

    def store(self, data, parent=-1):
        pass
        """
        c = self.cursor or self.get_cursor()

        _type = TYPE_MAP.get(type(data))

        c.execute(SQL_INSERT, (parent, _type, json.dumps(data),))

        return c.lastrowid
        """
 
    def feed(self, data, parent=-1):
        """Append data to the specified parent.

        Parent may be a dict or list.

        Returns list of ids:
        * value.id when appending {key : value} to a dict
        * each_child.id when appending a list to a list
        """
        # TODO: should be in a transaction
        id_list, pending_list = self._feed(data, parent)
        self.backend.batch_insert(pending_list)
        return id_list

    def _feed(self, data, parent_id=-1):
        parent = self.get_row(parent_id)
        parent_type = parent['type']

        id_list = []
        pending_list = []
 
        _type = TYPE_MAP.get(type(data))

        if _type == DICT:
            if parent_type == DICT:
                hash_id = parent_id
            elif parent_type in (LIST, KEY):
                hash_id = self.backend.insert((parent_id, _type, '',))
            elif parent_id != -1:
                print parent_id, parent_type
                raise IllegalTypeError, 'Parent node should be either DICT or LIST.'

            for key, value in data.iteritems():
                if key == self.link_key:
                    self.backend.update_link((value, hash_id))
                    continue
                key_id = self.backend.insert((hash_id, KEY, key,))
                _ids, _pendings = self._feed(value, key_id)
                id_list += _ids
                pending_list += _pendings

        elif _type == LIST:
            # TODO: need to distinguish *appending* from *merging* 
            #       now we always assume it's appending
            # TODO: The value field of LIST type is unused now.
            #       can be used to store the length.
            hash_id = self.backend.insert((parent_id, _type, '',))
            id_list.append(hash_id)
            for x in data:
                _ids, _pendings = self._feed(x, hash_id)
                id_list += _ids
                pending_list += _pendings
        else:
            pending_list.append((parent_id, _type, data,))

        return id_list, pending_list

    def query(self, path, parent=-1, one=False):
        rslt = self.backend.jsonpath(path=path, parent=parent, one=one)
        return QueryResult(rslt)

    xpath = query

    def build_node(self, row):
        node = get_initial_data(row['type'])
        _type = row['type']
        _value = row['value']

        if _type == KEY:
            for child in self.get_children(row['id']):
                node = {_value : self.build_node(child)}
                break

        elif _type in (LIST, DICT):
            func = node.update if _type == DICT else node.append
            for child in self.get_children(row['id']):
                func(self.build_node(child))

        elif _type == STR:
            node = _value

        elif _type == UNICODE:
            node = _value

        elif _type == BOOL:
            node = bool(_value)

        elif _type == INT:
            node = int(_value)

        elif _type == FLOAT:
            node = float(_value)

        elif _type == NIL:
            node = None

        return node

    def dumps(self):
        """Dump the json data"""
        root = self.get_row(-1)
        return self.build_node(root) if root else ''

    def dump(self, filepath):
        """Dump the json data to a file"""
        with open(filepath, 'wb') as f:
            root = self.get_row(-1)
            rslt = self.build_node(root) if root else ''
            f.write(repr(rslt))

    def dumprows(self):
        for row in self.backend.dumprows():
            print row

    def commit(self):
        self.backend.commit()

    def close(self):
        self.backend.close()

    def set_value(self, id, value):
        self.backend.set_value(id, value)

    def update_link(self, rowid, link=None):
        self.backend.update_link(rowid, link)

    def get_row(self, rowid):
        return self.backend.get_row(rowid)

    def get_dict_items(self, parent_id, value, cond='', order='asc', extra=''):
        for row in self.backend.iget_dict_items(parent_id=parent_id, value=value, cond=cond, order=order, extra=extra):
            yield row

    def get_children(self, parent_id, value=None, only_one=False):
        for row in self.backend.iget_children(parent_id, value=value, only_one=only_one):
            yield row

    def get_path(self):
        return self.backend.get_path()

