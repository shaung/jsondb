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
import weakref

try:
    import simplejson as json
except:
    import json

from datatypes import *
import backends
import jsonquery

__version__  = '0.1'


class Error(Exception):
    pass


class UnsupportedTypeError(Error):
    pass


class IllegalTypeError(Error):
    pass


class UnsupportedOperation(Error):
    pass


class QueryResult(object):
    def __init__(self, seq):
        self.seq = seq

    def getone(self):
        return next(self.seq)

    def itervalues(self):
        for row in self.seq:
            yield row.value

    def values(self):
        return list(row.value for row in self.seq)

    def __iter__(self):
        for row in self.seq:
            yield row


class Queryable(object):
    def __init__(self,  backend, link_key=None, root=-1):
        self.backend = weakref.proxy(backend)
        self.link_key = link_key or '@__link__'
        self.query_path_cache = {}
        self.root = root

    def __getitem__(self, key):
        if isinstance(key, basestring):
            if key.startswith('$'):
                return self.query(key)
            else:
                key = '$.%s' % key
                node = self.query(key).getone()
                root_id = node.id
                result = JsonDB(backend=self.backend, link_key=self.link_key, root=root_id)
                return result
        elif isinstance(key, (int, long)):
            return self.get_row(key)
        else:
            raise UnsupportedOperation

    def __setitem__(self, key, value):
        if isinstance(key, basestring):
            if key.startswith('$'):
                # == feed
                pass
            else:
                # == feed
                pass
        elif isinstance(key, (int, long)):
            self.feed(value, key)
        else:
            # TODO: slicing
            raise UnsupportedOperation

    def store(self, data, parent=-1):
        # TODO: store raw json data into a node.
        raise NotImplemented
 
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
        parent = self.backend.get_row(parent_id)
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
                    self.backend.update_link(hash_id, value)
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
        """Query the data"""
        cache = self.query_path_cache.get(path)
        if cache:
            ast = json.loads(cache)
        else:
            ast = jsonquery.parse(path)
            self.query_path_cache[path] = json.dumps(ast)
        rslt = self.backend.jsonpath(ast=ast, parent=parent, one=one)
        return QueryResult(rslt)

    xpath = query

    def build_node(self, row):
        node = get_initial_data(row['type'])
        _type = row['type']
        _value = row['value']

        if _type == KEY:
            for child in self.backend.iter_children(row['id']):
                node = {_value : self.build_node(child)}
                break

        elif _type in (LIST, DICT):
            func = node.update if _type == DICT else node.append
            for child in self.backend.iter_children(row['id']):
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
        root = self.backend.get_row(-1)
        return self.build_node(root) if root else ''

    def dump(self, filepath):
        """Dump the json data to a file"""
        with open(filepath, 'wb') as f:
            root = self.backend.get_row(-1)
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
        row = self.backend.get_row(rowid)
        if not row:
            return None
        return Result.from_row(row)

    def get_path(self):
        return self.backend.get_path()


class JsonDB(Queryable):
    @classmethod
    def create(cls, data={}, path=None, overwrite=True, link_key=None, **kws):
        """
        Create a new empty DB.

        :param data: Initial data. An empty dict if not specified.

        :param path: An RFC-1738-style string which specifies the URL to store into.

        :param overwrite: If is True and the database specified by *path* already exists, it will be truncated.

        :param link_key: Key directive for links in the database.

        :param kws: Additional parameters to parse to the engine.
        """
        _backend = backends.create(path, overwrite=overwrite)
        self = cls(backend=_backend, link_key=link_key)

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

        self.backend.insert_root((root_type, root))

        if root_type == DICT:
            for k, v in data.iteritems():
                self.feed({k:v})
        elif root_type == LIST:
            for x in data:
                self.feed(x)

        self.commit()

        return self

    @classmethod
    def load(cls, path, **kws):
        """
        Load from an existing DB.

        :param path: An RFC-1738-style string which specifies the URL to load from.

        :param kws: Additional parameters to parse to the engine.
        """
        _backend = backends.create(path, overwrite=False)
        self = cls(backend=_backend, link_key='')
        return self

    @classmethod
    def from_file(cls, dbpath, filepath, **kws):
        """Create a new db from json file"""
        if isinstance(filepath, basestring):
            fileobj = open(filepath)
        else:
            fileobj = filepath
        # TODO: streaming
        data = json.load(fileobj)

        self = cls.create(path=dbpath, **kws)
        try:
            self.feed(data)
        except:
            self.backend.rollback()
        else:
            self.commit()
        return self

    def set_link_key(self, link_key):
        self.link_key = link_key

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

