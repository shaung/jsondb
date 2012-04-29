# -*- coding: utf-8 -*-
# cython: profile=True

"""
    jsondb
    ~~~~~~

    Streaming JSON data from file.
    Query using JSONPath.
    Could be useful when the data is too large to fit the memory.
"""

import os
import tempfile
from collections import namedtuple

try:
    import simplejson as json
except:
    import json

import re
from constants import *
import backends

__version__  = '0.1'


class Error(Exception):
    pass


class UnsupportedTypeError(Error):
    pass


Result = namedtuple('Result', ('id', 'value', 'link'))

#def class JsonDB(object):
cdef class JsonDB:
    cdef public backend
    cdef public link_key

    def __init__(self,  backend, link_key=None):
        #self.conn = None
        #self.cursor = None
        self.backend = backend
        assert self.backend is not None
        self.link_key = link_key or '@__link__'

    def get_cursor(self):
        return self.backend.get_cursor()

    @classmethod
    def load(cls, path, **kws):
        # TODO: path -> connstr
        _backend = backends.create('sqlite3', filepath=path, overwrite=False)
        self = cls(backend=_backend, link_key='')
        return self

    @classmethod
    def create(cls, path=None, root_type=DICT, value=None, overwrite=True, link_key=None, backend_name='sqlite3'):
        if not path:
            fd, path = tempfile.mkstemp(suffix='.jsondb')
        dbpath = os.path.normpath(path)
        _backend = backends.create(backend_name, filepath=dbpath, overwrite=overwrite)
        self = cls(backend=_backend, link_key=link_key)

        if root_type in (BOOL, INT):
            value = int(value)
        elif root_type == FLOAT:
            value = float(value)

        self.backend.insert_root((root_type, value))
        self.commit()

        return self

    def set_value(self, id, value):
        pass
        """
        c = self.cursor or self.get_cursor()
        c.execute(SQL_UPDATE_VALUE, (value, id))
        """

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
            else:
                hash_id = self.backend.insert((parent_id, _type, '',))
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
            hash_id = self.backend.insert((parent_id, _type, '',))
            id_list.append(hash_id)
            for x in data:
                _ids, _pendings = self._feed(x, hash_id)
                id_list += _ids
                pending_list += _pendings
                #print 'added list item to %s' % (hash_id)
        else:
            #c.execute(SQL_INSERT, (parent_id, _type, data,))
            pending_list.append((parent_id, _type, data,))
            # TODO: what?
            #id_list.append(c.lastrowid)
            #print 'added other item %s to %s' % (id_list[0], parent_id)

        return id_list, pending_list
        #self.conn.commit()

    def break_path(self, path):
        # here we ignore the '..' expr for now
        path = path[2:]
        conditions = []
        def func(m):
            rslt = '__%s__' % len(conditions)
            conditions.append(m.group(0))
            return rslt

        normed = re.sub(r'\[(.*?)\]', func, path)
        groups = normed.split('.')

        def recover(m):
            return conditions[int(m.group(1))]
        return [re.sub(r'__([0-9]*)__', recover, g) for g in groups]

    def _get_cond(self, expr):
        cond = ''
        extra = ''
        order = 'asc'
        reverse = False
        #print expr
        if expr[-1] == ']':
            name, cond = expr[:-1].split('[')
            if cond.startswith('?'):
                extra = ''
                # for sub query
                cond = cond[2:-1]

                def f(m):
                    item = m.group(1)
                    condition = m.group(2)
                    # TODO: quick dirty adhoc solution
                    condition = condition.replace('True', '1').replace('False', '0').replace('"', "'")
                    return """ exists (select tv.id from jsondata tv
                                where tv.parent in (select tk.id from jsondata tk 
                                where tk.value = '%s' and tk.parent = t.id and tk.type = %s ) and tv.value %s )""" % (item, KEY, condition)

                # break ands and ors
                conds = []
                for _and in cond.split(' and '):
                    conds.append(' or '.join(re.sub(r'@\.(\w+)(.*)', f, _or) for _or in _and.split(' or ')))
                cond = ' and %s ' % (' and '.join(conds))
            else:
                # for lists
                while cond.startswith('(') and cond.endswith(')'):
                    cond = cond[1:-1]
                if cond and cond != '*':
                    if ':' not in cond:
                        nth = int(cond)
                        if nth < 0:
                            order = 'desc'
                            nth *= -1
                            nth -= 1
                        extra = 'limit 1 offset %s' % nth
                    else:
                        nstart, nend = [x.strip() for x in cond.split(':')]
                        if nstart or nend:
                            if nstart and nend:
                                # [1:1]
                                # [-2:-1]
                                nstart = int(nstart)
                                nend = int(nend)
                                limit = nend - nstart + 1
                                if nstart < 0:
                                    nstart *= -1
                                    order = 'desc'
                                extra = 'limit %s offset %s' % (limit, nstart)
                            else:
                                if nstart:
                                    # [0:]
                                    # [-1:]
                                    nstart = int(nstart)
                                    if nstart < 0:
                                        nstart *= -1
                                        order = 'desc'
                                    extra = 'limit %s offset %s' % (nstart, nstart - 1)
                                elif nend:
                                    nend = int(nend)
                                    if nend >= 0:
                                        # [:1]
                                        extra = 'limit %s' % (nend + 1)
                                    else:
                                        # TODO: the order can not be done directly.
                                        # [:-1]
                                        order = 'desc'
                                        extra = 'limit -1 offset %s' % (nend * -1)
                                        reverse = True
                cond = ''
        else:
            name = expr
        return name, cond, extra, order, reverse

    def xpath(self, path, parent=-1, one=False):
        #print 'jsondb.path', path
        paths = self.break_path(path) if '.' in path[2:] else [path[2:]]

        parent_ids = [parent]

        # TODO
        with self.backend.get_connection():
            for i, expr in enumerate(paths[:-1]):
                if '[' in expr:
                    name, cond, extra, order, reverse = self._get_cond(expr)
                else:
                    name, cond, extra, order, reverse = expr, '', '', 'asc', False
                #print i, name, cond, extra, order, reverse
                new_parent_ids = []
                for parent_id in parent_ids:
                    row = self.get_dict_items(parent_id, name, cond=cond, order=order, extra=extra)
                    ids = [r['id'] for r in row]
                    if reverse:
                        ids = reversed(ids)
                    new_parent_ids += ids
                parent_ids = new_parent_ids
                if not parent_ids:
                    return
        
            #print paths[-1]
            expr = paths[-1]
            if '[' in expr:
                name, cond, extra, order, reverse = self._get_cond(expr)
            else:
                name, cond, extra, order, reverse = expr, '', '', 'asc', False
            #print name, cond, extra, order, reverse
            if reverse:
                for x in reversed(sum(([Result(row['id'], row['value'] if row['type'] != BOOL else bool(row['value']), row['link']) for row in self.get_dict_items(parent_id, value=name, cond=cond, order=order, extra=extra) if row] for parent_id in parent_ids), [])):
                    yield x
                return

            for parent_id in parent_ids:
                for row in self.get_dict_items(parent_id, value=name, cond=cond, order=order, extra=extra):
                    rslt = Result(row['id'], row['value'] if row['type'] != BOOL else bool(row['value']), row['link'])
                    yield rslt
                    if one:
                        return

    def update_link(self, rowid, link=None):
        self.backend.update_link(rowid, link)

    def set_link_key(self, link_key):
        self.link_key = link_key

    def get_row(self, rowid):
        return self.backend.get_row(rowid)

    def get_dict_items(self, parent_id, value, cond='', order='asc', extra=''):
        for row in self.backend.iget_dict_items(parent_id=parent_id, value=value, cond=cond, order=order, extra=extra):
            yield row

    def get_children(self, parent_id, value=None, only_one=False):
        for row in self.backend.iget_children(parent_id, value=value, only_one=only_one):
            yield row

    def build_node(self, row):
        node = get_initial_data(row['type'])
        _type = row['type']

        if _type == KEY:
            for child in self.get_children(row['id']):
                node = {row['value'] : self.build_node(child)}
                break

        elif _type in (LIST, DICT):
            func = node.update if _type == DICT else node.append
            for child in self.get_children(row['id']):
                func(self.build_node(child))

        elif _type == STR:
            node = row['value']

        elif _type == UNICODE:
            node = row['value']

        elif _type == BOOL:
            node = bool(row['value'])

        elif _type == INT:
            node = int(row['value'])

        elif _type == FLOAT:
            node = float(row['value'])

        elif _type == NIL:
            node = None

        return node

    @classmethod
    def from_file(cls, dbpath, filepath, **kws):
        data = json.load(open(filepath))
        _type = TYPE_MAP.get(type(data))
        self = cls.create(dbpath, root_type=_type, **kws)
        with self.backend.get_connection():
            self.feed(data)
            self.commit()
        return self

    def dumps(self):
        root = self.get_row(-1)
        return self.build_node(root) if root else ''

    def dump(self, filepath):
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
