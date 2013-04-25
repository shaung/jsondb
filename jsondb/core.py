# -*- coding: utf-8 -*-

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

import logging
logger = logging.getLogger(__file__)

try:
    import simplejson as json
except:
    import json

from datatypes import *
import backends
import jsonquery

from error import *


class Nothing:
    pass


def get_type_class(datatype):
    if datatype == DICT:
        cls = DictQueryable
    elif datatype == LIST:
        cls = ListQueryable
    elif datatype in (STR, UNICODE):
        cls = StringQueryable
    elif datatype == FLOAT:
        cls = NumberQueryable
    elif datatype in (INT, BOOL):
        cls = IntegerQueryable
    else:
        cls = PlainQueryable

    return cls


class QueryResult(object):
    def __init__(self, seq, queryset):
        self.seq = seq
        self.queryset = weakref.proxy(queryset)

    def getone(self):
        try:
            row = next(self.seq)
            return self.queryset._make(row.id, row.type)
        except StopIteration:
            return None

    def itervalues(self):
        for row in self.seq:
            yield self.queryset._make(row.id, row.type).data()

    def values(self):
        return list(self.itervalues())

    def __iter__(self):
        for row in self.seq:
            yield self.queryset._make(row.id, row.type)


class Queryable(object):
    def __init__(self, backend, link_key=None, root=-1, datatype=None, data=Nothing()):
        self.backend = weakref.proxy(backend) if isinstance(backend, weakref.ProxyTypes) else backend
        self.link_key = link_key or '@__link__'
        self.query_path_cache = {}
        self.root = root
        self._data = data
        self.datatype = datatype

    def __hash__(self):
        return self.root

    def __len__(self):
        raise NotImplementedError

    def __getattr__(self, name):
        cls = self.get_datatype()
        try:
            attr = getattr(cls, name)
        except:
            raise
        else:
            return getattr(self.data(), name)

    def __cmp__(self, other):
        if isinstance(other, Queryable):
            if other.datatype != self.datatype:
                return -1
            other_data = other.data()
        else:
            other_data = other

        self_data = self.data()
        if self_data == other_data:
            return 0
        else:
            return 1 if self_data > other_data else -1

    def _make(self, id, type=None):
        if type is None:
            row = self.backend.get_row(id)
            type = row['type']

        cls = get_type_class(type)

        if type in (LIST, DICT):
            row = self.backend.get_row(id)
            data = row['value']
        else:
            data = Nothing()
        result = cls(backend=self.backend, link_key=self.backend.get_link_key(), root=id, datatype=type, data=data)
        return result

    def __getitem__(self, key):
        """
        Same with query, but mainly for direct access.
        1. Does not require the $ prefix
        2. Only query for direct children
        3. Only retrive one child
        """
        if isinstance(key, bool):
            raise UnsupportedOperation
        elif isinstance(key, basestring):
            if not key.startswith('$'):
                key = '$.%s' % key
        elif isinstance(key, (int, long)):
            key = '$.[%s]' % key
        else:
            raise UnsupportedOperation

        node = self.query(key, one=True).getone()
        return node

    def store(self, data, parent=None):
        # TODO: store raw json data into a node.
        raise NotImplementedError

    def from_file(self, f):
        """ Load from a json file."""
        if isinstance(f, basestring):
            f = open(f, 'rd')

        data = json.load(f)
        self.feed(data)

        f.close()

    def feed(self, data, parent=None):
        """Append data to the specified parent.

        Parent may be a dict or list.

        Returns list of ids:
        * value.id when appending {key : value} to a dict
        * each_child.id when appending a list to a list
        """
        if parent is None:
            parent = self.root
        # TODO: should be in a transaction
        id_list, pending_list = self._feed(data, parent)
        self.backend.batch_insert(pending_list)
        return id_list

    def _feed(self, data, parent_id, real_parent_id=None):
        parent = self.backend.get_row(parent_id)
        parent_type = parent['type']

        if real_parent_id is None:
            real_parent_id = parent_id

        id_list = []
        pending_list = []

        _type = TYPE_MAP.get(type(data))

        if _type == DICT:
            if parent_type == DICT:
                hash_id = parent_id
            elif parent_type in (LIST, KEY):
                hash_id = self.backend.insert((parent_id, _type, 0,))
            elif parent_id != self.root:
                print parent_id, parent_type
                raise IllegalTypeError('Parent node should be either DICT or LIST.')

            for key, value in data.iteritems():
                if key == self.link_key:
                    self.backend.update_link(hash_id, value)
                    continue
                key_id, value_id = self.backend.find_key(key, hash_id) if parent_type == DICT else (None, None)
                if key_id is not None:
                    self.backend.remove(key_id)
                else:
                    key_id = self.backend.insert((hash_id, KEY, key,))
                _ids, _pendings = self._feed(value, key_id, real_parent_id=hash_id)
                id_list += _ids
                pending_list += _pendings

        elif _type == LIST:
            # TODO: need to distinguish *appending* from *merging*
            #       now we always assume it's appending
            hash_id = self.backend.insert((parent_id, _type, 0,))
            id_list.append(hash_id)
            for x in data:
                _ids, _pendings = self._feed(x, hash_id)
                id_list += _ids
                pending_list += _pendings
        else:
            pending_list.append((parent_id, _type, data,))

        return id_list, pending_list

    def query(self, path, parent=None, one=False):
        """
        Query the data.
        """
        if parent is None:
            parent = self.root

        cache = self.query_path_cache.get(path)
        if cache:
            ast = json.loads(cache)
        else:
            ast = jsonquery.parse(path)
            self.query_path_cache[path] = json.dumps(ast)
        rslt = self.backend.jsonpath(ast=ast, parent=parent, one=one)
        return QueryResult(rslt, self)

    xpath = query

    def build_node(self, row):
        node = get_initial_data(row['type'])
        _type = row['type']
        _value = row['value']

        if _type == KEY:
            for child in self.backend.iter_children(row['id']):
                node = {_value: self.build_node(child)}
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

    def id(self):
        return self.root

    def get_datatype(self):
        return get_datatype_class(self.datatype)

    def data(self, update=False):
        if not self.datatype in (LIST, DICT):
            if not update and not isinstance(self._data, Nothing):
                return self._data
        root = self.backend.get_row(self.root)
        _data = self.build_node(root) if root else DATA_INITIAL[self.datatype]
        if not self.datatype in (LIST, DICT):
            self._data = _data
        return _data

    def link(self):
        root = self.backend.get_row(self.root)
        return root['link']

    def check_type(self, data):
        new_type = TYPE_MAP.get(type(data))
        if self.datatype in (LIST, DICT):
            return TYPE_MAP.get(type(data)) == self.datatype
        return True

    def _update(self, data):
        new_type = TYPE_MAP.get(type(data))
        if self.datatype in (LIST, DICT):
            self.backend.remove(self.root)

        if new_type in (LIST, DICT):
            self._data = len(data)
        else:
            self._data = data

        if self.datatype == new_type:
            self.backend.set_value(self.root, self._data)
        else:
            self.backend.set_row(self.root, new_type, self._data)
            self.datatype = new_type

        if new_type == LIST:
            for x in data:
                self.feed(x)
        elif new_type == DICT:
            for k, v in data.iteritems():
                self.feed({k: v})
        else:
            self.backend.set_value(self.root, data)

        import jsondb
        cls = get_type_class(new_type)
        if isinstance(self, jsondb.BaseDB):
            self.__class__.__base__[0] = cls
        else:
            self.__class__ = cls

    _ = property(data, _update)

    def dumps(self):
        """Dump the json data"""
        return json.dumps(self.data())

    def dump(self, filepath):
        """Dump the json data to a file"""
        with open(filepath, 'wb') as f:
            root = self.backend.get_row(self.root)
            rslt = self.build_node(root) if root else ''
            f.write(repr(rslt))

    def commit(self):
        self.backend.commit()

    def close(self):
        self.backend.close()

    def set_value(self, id, value):
        self.backend.set_value(id, value)

    def _get_value(self):
        row = self.backend.get_row(self.root)
        data = row['value']
        return data

    def update_link(self, rowid, link=None):
        self.backend.update_link(rowid, link)

    def get_row(self, rowid):
        row = self.backend.get_row(rowid)
        if not row:
            return None
        return Result.from_row(row)

    def get_path(self):
        return self.backend.get_path()

    def get_url(self):
        return self.backend.get_url()


class SequenceQueryable(Queryable):
    def __len__(self):
        return self.backend.get_children_count(self.root)

    def __getitem__(self, key):
        if isinstance(key, slice):
            rslt = self.backend.iter_slice(self.root, key.start, key.stop, key.step)
            return QueryResult(rslt, self)
        return super(SequenceQueryable, self).__getitem__(key)

    def __setitem__(self, key, value):
        """
        If the value associated to the key already exists, it will be override by value.

        :param key: key to set data

        :param value: value to set to key
        """

        if isinstance(value, Queryable):
            return

        node = self[key]
        if node:
            self.backend.remove(node.root)

        if self.datatype == DICT:
            self.update({key: value})
        elif self.datatype == LIST and isinstance(key, (int, long)):
            if abs(key) >= len(self):
                raise IndexError
            node.feed(value)
            # TODO: feed list
        else:
            node._update(value)

    def __delitem__(self, key):
        if self.datatype in (DICT, KEY):
            key_id, _ = self.backend.find_key(key, self.root)
            if key_id is not None:
                self.backend.remove(key_id, include_self=True)
        elif self.datatype == LIST:
            if isinstance(key, (int, long)):
                node = self[key]
                if node:
                    self.backend.remove(node.root, include_self=True)
            elif isinstance(key, slice):
                for node in self.backend.iter_slice(self.root, key.start, key.stop, key.step):
                    self.backend.remove(node)
        else:
            raise UnsupportedTypeError

    def __iter__(self):
        for x in self.backend.iter_slice(self.root):
            yield self._make(x)

    def __reversed__(self):
        for x in self.backend.iter_slice(self.root, None, None, -1):
            yield self._make(x)

    def __contains__(self, item):
        # FIXME: This would be very slow
        return item in self.data()

    def __add__(self, other):
        return self.data() + other

    def __radd__(self, other):
        return other + self.data()

    def max(self):
        ids = (x for x in self.backend.iter_slice(self.root))
        return max(self.backend.get_row(id)['value'] for id in ids)

    def min(self):
        ids = (x for x in self.backend.iter_slice(self.root))
        return min(self.backend.get_row(id)['value'] for id in ids)


class ListQueryable(SequenceQueryable):
    def append(self, data):
        self.feed(data)

    def __getitem__(self, key):
        if isinstance(key, (int, long)):
            if abs(key) >= len(self):
                raise IndexError
            rslt = self.backend.get_nth_child(self.root, key)
            return self._make(rslt.id, rslt.type)
        return super(ListQueryable, self).__getitem__(key)

    def __mul__(self, other):
        return self.data() * other

    __rmul__ = __mul__

    def __iadd__(self, other):
        for data in other:
            self.feed(data)
        return self

    def __imul__(self, times):
        if times <= 0:
            self.backend.remove(self.root)
        else:
            data = self.data()
            for i in range(times - 1):
                for item in data:
                    self.feed(item)

        return self


class DictQueryable(SequenceQueryable):
    def update(self, data):
        self.feed(data)

    def clear(self):
        self.backend.remove(self.root)

    def get(self, key, default=None):
        result = self[key]
        return result.data() if result else default

    def items(self):
        return self.data().items()

    def iteritems(self):
        for key, value_row in self.backend.iter_dict(self.root):
            yield key, self._make(value_row.id, value_row.type).data()

    def __contains__(self, item):
        key_id, _ = self.backend.find_key(item, self.root)
        return key_id is not None


class PlainQueryable(Queryable):
    def __iadd__(self, other):
        data = self.__add__(other)
        self._update(data)
        return self

    def __isub__(self, other):
        data = self.__sub__(other)
        self._update(data)
        return self

    def __imul__(self, other):
        data = self.__mul__(other)
        self._update(data)
        return self

    def __ifloordiv__(self, other):
        data = self.__floordiv__(other)
        self._update(data)
        return self

    def __idiv__(self, other):
        data = self.__div__(other)
        self._update(data)
        return self

    def __itruediv__(self, other):
        pass

    def __imod_(self, other):
        data = self.__mod__(other)
        self._update(data)
        return self

    def __ipow__(self, other):
        data = self.__pow__(other)
        self._update(data)
        return self

    def __ilshift__(self, other):
        data = self.__lshift__(other)
        self._update(data)
        return self

    def __irshift__(self, other):
        data = self.__rshift__(other)
        self._update(data)
        return self

    def __iand__(self, other):
        data = self.__and__(other)
        self._update(data)
        return self

    def __ior__(self, other):
        data = self.__or__(other)
        self._update(data)
        return self

    def __ixor__(self, other):
        data = self.__xor__(other)
        self._update(data)
        return self


class StringQueryable(PlainQueryable, SequenceQueryable):
    def __len__(self):
        return len(self.data())

    def __add__(self, other):
        return self.data() + other

    def __radd__(self, other):
        return other + self.data()

    def __mul__(self, other):
        return self.data() * other

    __rmul__ = __mul__

    def __mod__(self, data):
        return self.data() % data

    def __contains__(self, item):
        return item in self.data()

    def index(self, item):
        return self.data().index(item)

    def count(self, item):
        return self.data().count(item)

    def __iter__(self):
        return iter(self.data())

    def __getitem__(self, key):
        return self.data()[key]

    def __setitem__(self, key, value):
        if isinstance(value, StringQueryable):
            return
        data = self.data()
        if isinstance(key, (int, long)):
            if len(value) == 1:
                data = '%s%s%s' % (data[:key], value, data[key+1:])
        elif isinstance(key, slice):
            chars = list(data)
            chars[key] = list(value)
            data = ''.join(chars)
        self._update(data)


class NumberQueryable(PlainQueryable):
    def __nonzero__(self):
        return self.data()

    def __pos__(self):
        return self.data().__pos__()

    def __neg__(self):
        return self.data().__neg__()

    def __abs__(self):
        return self.data().__abs__()

    def __add__(self, other):
        return self.data() + other

    def __sub__(self, other):
        return self.data() - other

    def __mul__(self, other):
        return self.data() * other

    def __floordiv__(self, other):
        return self.data() // other

    def __div__(self, other):
        return self.data() / other

    def __truediv__(self, other):
        return self.data().__truediv__(other)

    def __mod__(self, other):
        return self.data() % other

    def __pow__(self, other):
        return self.data() ** other

    def __radd__(self, other):
        return self.__add__(other)

    def __rsub__(self, other):
        return other - self.data()

    def __rmul__(self, other):
        return self.__mul__(other)

    def __rfloordiv__(self, other):
        return other // self.data()

    def __rdiv__(self, other):
        return other // self.data()

    def __rtruediv__(self, other):
        pass

    def __rmod__(self, other):
        return other % self.data()

    def __rpow__(self, other):
        return other ** self.data()


class IntegerQueryable(NumberQueryable):
    def __invert__(self):
        return self.data().__invert__()

    def __lshift__(self, other):
        return self.data() << other

    def __rshift__(self, other):
        return self.data() >> other

    def __and__(self, other):
        return self.data() & other

    def __or__(self, other):
        return self.data() | other

    def __xor__(self, other):
        return self.data() ^ other

    def __rlshift__(self, other):
        return other << self.data()

    def __rrshift__(self, other):
        return other >> self.data()

    def __rand__(self, other):
        return self.__and__(other)

    def __ror__(self, other):
        return self.__or__(other)

    def __rxor__(self, other):
        return self.__xor__(other)


class EmptyNode(Queryable):
    pass
