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
import sqlite3
import simplejson
import re
import types


__version__  = '0.1'


(INT, FLOAT, STR, UNICODE, BOOL, NULLTYPE, LIST, DICT, KEY) = DATA_TYPES = range(9)
DATA_INITIAL = {
    INT     : 0,
    FLOAT   : 0.0,
    STR     : '',
    UNICODE : u'',
    BOOL    : False,
    NULLTYPE    : None,
    LIST    : [],
    DICT    : {},
    KEY     : '',
}

DATA_TYPE_NAME = {
    INT     : 'INT',
    FLOAT   : 'FLOAT',
    STR     : 'STR',
    UNICODE : 'UNICODE',
    BOOL    : 'BOOL',
    NULLTYPE    : 'NULLTYPE',
    LIST    : 'LIST',
    DICT    : 'DICT',
    KEY     : 'KEY',
}

TYPE_MAP = {
    types.IntType     : INT,
    types.FloatType   : FLOAT,
    types.StringType  : STR,
    types.UnicodeType : UNICODE,
    types.BooleanType : BOOL,
    types.NoneType    : NULLTYPE,
    types.ListType    : LIST,
    types.DictType    : DICT,
}

SQL_INSERT_ROOT = "insert into jsondata values(-1, -2, ?, ?, null)"
SQL_INSERT = "insert into jsondata values(null, ?, ?, ?, null)"
SQL_UPDATE_LINK = "update jsondata set link = ? where id = ?"

SQL_SELECT_DICT_ITEMS = "select id, type, value, link from jsondata where parent in (select distinct id from jsondata where parent = ? and type = %s and value = ?) order by id asc" % KEY

SQL_SELECT_CHILDREN = "select id, type, value, link from jsondata where parent = ? order by id asc"
SQL_SELECT_CHILDREN_COND = "select t.id, t.type, t.value, t.link from jsondata t where t.parent = ? %s order by t.id %s %s"

SQL_SELECT = "select * from jsondata where id = ?"


class Error(Exception):
    pass


class UnsupportedTypeError(Error):
    pass


def get_initial_data(_type):
    if _type == NULLTYPE:
        return None
    cls = DATA_INITIAL[_type].__class__
    return cls.__new__(cls)


from functools import wraps
import time


Result = namedtuple('Result', ('id', 'value', 'link'))

cdef class JsonDB:
    cdef public dbpath
    cdef conn, cursor, link_key

    def __init__(self, filepath=None, link_key=None):
        self.conn = None
        self.cursor = None
        if not filepath:
            fd, filepath = tempfile.mkstemp(suffix='.jsondb')
        self.dbpath = os.path.normpath(filepath)
        self.link_key = link_key or '@__link__'

    cdef inline get_cursor(self):
        if not self.cursor or not self.conn:
            conn = self.conn or self.get_connection()
            try:
                self.cursor.close()
            except:
                pass
            self.cursor= conn.cursor()
        return self.cursor

    cdef inline get_connection(self, force=False):
        if force or not self.conn:
            try:
                self.conn.close()
            except:
                pass

            self.conn = sqlite3.connect(self.dbpath)
            self.conn.row_factory = sqlite3.Row
            self.conn.text_factory = str
            self.conn.execute('PRAGMA encoding = "UTF-8";')
            self.conn.execute('PRAGMA foreign_keys = ON;')
            self.conn.execute('PRAGMA synchronous = OFF;')
            self.conn.execute('PRAGMA page_size = 8192;')
            self.conn.execute('PRAGMA automatic_index = 1;')
            self.conn.execute('PRAGMA temp_store = MEMORY;')
            self.conn.execute('PRAGMA journal_mode = MEMORY;')

        return self.conn

    cdef create_tables(self):
        conn = self.get_connection()

        # create tables
        conn.execute("""create table if not exists jsondata
        (id     integer primary key,
         parent integer,
         type   integer,
         value  blob,
         link   text
        )""")

        conn.execute("create index if not exists jsondata_idx_composite on jsondata (parent, type)")
        #conn.execute("create index if not exists jsondata_idx_value on jsondata (value)")
        #conn.execute("create index if not exists jsondata_idx_type on jsondata (type asc)")
 
        conn.commit()

    def build_index(self):
        conn = self.get_connection()
        #conn.execute("create index if not exists jsondata_idx_parent on jsondata (parent asc)")
        #conn.execute("create index if not exists jsondata_idx_type on jsondata (type asc)")
        #conn.execute("create index if not exists jsondata_idx_value on jsondata (parent, value)")
        conn.execute("analyze")
        conn.commit()

    def _get_hash_id(self, name):
        c = self.cursor or self.get_cursor()
        c.execute('''select max(id) as max_id from jsondata
        ''')
        max_id = c.fetchone()['max_id']
        return max_id + 1 if max_id else 1

    @classmethod
    def load(cls, path, **kws):
        self = cls(path, **kws)
        return self

    @classmethod
    def create(cls, path=None, root_type=DICT, value=None, overwrite=True, link_key=None):
        self = cls(path, link_key=link_key)
        if overwrite:
            try:
                conn = self.conn or self.get_connection()
                conn.execute('drop table jsondata')
            except sqlite3.OperationalError:
                pass

        self.create_tables()

        if root_type == BOOL:
            value = int(value)
        elif root_type == INT:
            value = int(value)
        elif root_type == FLOAT:
            value = float(value)

        c = self.cursor or self.get_cursor()
        conn = self.conn
        c.execute(SQL_INSERT_ROOT, (root_type, value))
        conn.commit()

        return self
 
    cpdef feed(self, data, int parent_id=-1):
        """Append data to the specified parent.

        Parent may be a dict or list.

        Returns list of ids:
        * value.id when appending {key : value} to a dict
        * each_child.id when appending a list to a list
        """
        id_list, pending_list = self._feed(data, parent_id)
        c = self.cursor or self.get_cursor()
        c.executemany(SQL_INSERT, pending_list)
        return id_list

    cdef _feed(self, data, int parent_id=-1):
        c = self.cursor or self.get_cursor()

        parent = self.get_row(parent_id)
        parent_type = parent['type']

        id_list = []
        pending_list = []
 
        _type = TYPE_MAP.get(type(data))

        #print data, DATA_TYPE_NAME[_type], DATA_TYPE_NAME[parent_type]
        if _type == DICT:
            if parent_type == DICT:
                hash_id = parent_id
            else:
                c.execute(SQL_INSERT, (parent_id, _type, '',))
                hash_id = c.lastrowid
            for key, value in data.iteritems():
                if key == self.link_key:
                    c.execute(SQL_UPDATE_LINK, (value, hash_id))
                    continue
                c.execute(SQL_INSERT, (hash_id, KEY, key,))
                key_id = c.lastrowid
                #print 'added dict item %s to %s' % (key_id, hash_id)
                _ids, _pendings = self._feed(value, key_id)
                id_list += _ids
                pending_list += _pendings

        elif _type == LIST:
            # TODO: need to distinguish *appending* from *merging* 
            #       now we always assume it's appending
            c.execute(SQL_INSERT, (parent_id, _type, '',))
            hash_id = c.lastrowid
            id_list.append(hash_id)
            for x in data:
                _ids, _pendings = self._feed(x, hash_id)
                id_list += _ids
                pending_list += _pendings
                #print 'added list item to %s' % (hash_id)
        else:
            #c.execute(SQL_INSERT, (parent_id, _type, data,))
            pending_list.append((parent_id, _type, data,))
            id_list.append(c.lastrowid)
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

    def xpath(self, path, int node_id=-1, one=False):
        #print 'jsondb.path', path
        paths = self.break_path(path) if '.' in path[2:] else [path[2:]]
        c = self.cursor or self.get_cursor()

        parent_ids = [node_id]

        with self.conn:
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
                    return []
        
            #print paths[-1]
            expr = paths[-1]
            if '[' in expr:
                name, cond, extra, order, reverse = self._get_cond(expr)
            else:
                name, cond, extra, order, reverse = expr, '', '', 'asc', False
            #print name, cond, extra, order, reverse
            rslt = sum(([Result(row['id'], row['value'] if row['type'] != BOOL else bool(row['value']), row['link']) for row in self.get_dict_items(parent_id, value=name, cond=cond, order=order, extra=extra) if row] for parent_id in parent_ids), [])
            rslt = reversed(rslt) if reverse else rslt
            if one:
                return rslt[0] if rslt else None
            return rslt

    def update_link(self, rowid, link=None):
        c = self.cursor or self.get_cursor()
        c.execute('update jsondata set link = ? where id = ?', (link, rowid, ))

    def set_link_key(self, link_key):
        self.link_key = link_key

    def get_row(self, rowid):
        c = self.cursor or self.get_cursor()
        c.execute(SQL_SELECT, (rowid, ))
        rslt = c.fetchone()
        return rslt

    def get_dict_items(self, parent_id, value, cond='', order='asc', extra=''):
        c = self.cursor or self.get_cursor()
        
        rows = c.execute(SQL_SELECT_DICT_ITEMS + ' limit 1', (parent_id, value))
        for row in rows:
            #print 'row', row
            if row['type'] == LIST:
                sql = SQL_SELECT_CHILDREN_COND % (cond, order, extra)
                #print 'sql:%s' % sql
                for item in c.execute(sql, (row['id'],)):
                    yield item
            else:
                yield row

    def get_children(self, parent_id, value=None, only_one=False):
        c = self.cursor or self.get_cursor()

        sql = SQL_SELECT_CHILDREN
        paras = [parent_id]
        if value is not None:
            sql += ' and value = ? '
            paras.append(value)

        c.execute(sql, tuple(paras))
        if only_one:
            row = c.fetchone()
            yield row
        else:
            for row in  c.fetchall():
                yield row

    def build_node(self, row):
        node = get_initial_data(row['type'])
        _type = row['type']
        #print 'buiding node', row['id'], type(node)

        if _type == KEY:
            for child in self.get_children(row['id']):
                node = {row['value'] : self.build_node(child)}
                break

        elif _type in (LIST, DICT):
            func = node.update if _type == DICT else node.append
            for child in self.get_children(row['id']):
                #print 'add child', child
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

        elif _type == NULLTYPE:
            node = None

        return node

    @classmethod
    def from_file(cls, dbpath, filepath, **kws):
        json = simplejson.load(open(filepath))
        _type = TYPE_MAP.get(type(json))
        self = cls.create(dbpath, root_type=_type, **kws)
        with self.conn:
            self.feed(json)
        return self

    def dumps(self):
        root = self.get_row(-1)
        return self.build_node(root)

    def dumprows(self):
        c = self.cursor or self.get_cursor()
        c.execute('select * from jsondata order by id')
        fmt = '{0:>12} {1:>12} {2:12} {3:12}'
        print fmt.format('id', 'parent', 'type', 'value')
        for row in c.fetchall():
            print fmt.format(row['id'], row['parent'], DATA_TYPE_NAME[row['type']], row['value'])

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.commit()
            self.conn.close()

    def commit(self):
        self.conn.commit()

