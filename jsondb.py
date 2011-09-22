# -*- coding: utf-8 -*-

"""
    jsondb
    ~~~~~~

    Streaming JSON data from file.
    Query using JSONPath.
    Could be useful when the data is too large to fit the memory.
"""

import os
import tempfile
import sqlite3
import simplejson

import logging, traceback
logger = logging.getLogger(__file__)


(NUM, STR, UNICODE, BOOL, NULL, LIST, DICT, KEY) = DATA_TYPES = range(8)
DATA_INITIAL = {
    NUM     : 0,
    STR     : '',
    UNICODE : u'',
    BOOL    : False,
    NULL    : None,
    LIST    : [],
    DICT    : {},
    KEY     : '',
}

SQL_INSERT_ROOT = "insert into jsondata values(0, ?, ?, ?)"
SQL_INSERT = "insert into jsondata values(null, ?, ?, ?)"

SQL_SELECT_DICT_ITEMS = "select id, type, value from jsondata where parent in (select distinct id from jsondata where parent = ? and type = %s and value = ?)" % KEY

SQL_SELECT_CHILDREN = "select id, type, value from jsondata where parent = ?"

SQL_SELECT = "select * from jsondata where id = ?"


class Error(Exception):
    pass


class UnsupportedTypeError(Error):
    pass


def get_initial_data(_type):
    if _type == NULL:
        return None
    cls = DATA_INITIAL[_type].__class__
    return cls.__new__(cls)


def get_data_type(data):
    for _type, _data in DATA_INITIAL.iteritems():
        if type(_data) == type(data):
            return _type

    raise UnsupportedTypeError, repr(data)


from functools import wraps
import time


def timeit(f):
    @wraps(f)
    def wrap(*args, **kws):
        s = time.time()
        rslt = f(*args, **kws)
        e = time.time()
        print f.__name__, 'used %ssec' % (e - s)
        return rslt

    return wrap


class JsonDB(object):
    def __init__(self, filepath=None):
        self.conn = None
        self.cursor = None
        if not filepath:
            fd, filepath = tempfile.mkstemp(suffix='*.jsondb')
        self.dbpath = os.path.normpath(filepath)

    def get_cursor(self):
        if not self.cursor or not self.conn:
            conn = self.conn or self.get_connection()
            try:
                self.cursor.close()
            except:
                pass
            self.cursor= conn.cursor()
        return self.cursor

    def get_connection(self, force=False):
        if force or not self.conn:
            try:
                self.conn.close()
            except:
                pass

            self.conn = sqlite3.connect(self.dbpath, isolation_level=None)
            self.conn.row_factory = sqlite3.Row
            self.conn.text_factory = str
            self.conn.execute('PRAGMA encoding = "UTF-8";')
            self.conn.execute('PRAGMA foreign_keys = ON;')
            self.conn.execute('PRAGMA synchronous = OFF;')
            self.conn.execute('PRAGMA page_size = 8192;')
            self.conn.execute('PRAGMA automatic_index = 0;')
            self.conn.execute('PRAGMA temp_store = MEMORY;')
            self.conn.execute('PRAGMA journal_mode = MEMORY;')

        return self.conn

    def create_tables(self):
        conn = self.get_connection()

        # create tables
        conn.execute("""create table if not exists jsondata
        (id     integer primary key,
         parent integer,
         type   integer,
         value  text
        )""")

        conn.execute("""create index if not exists jsondata_idx on jsondata
        (parent asc,
         type   asc
        )""")

        conn.commit()

    def _get_hash_id(self, name):
        c = self.cursor or self.get_cursor()
        c.execute('''select max(id) as max_id from jsondata
        ''')
        max_id = c.fetchone()['max_id']
        return max_id + 1 if max_id else 1

    @classmethod
    @timeit
    def load(cls, path):
        self = cls(path)
        return self

    @classmethod
    def create(cls, path=None, root_type=DICT, value=None, overwrite=True):
        self = cls(path)
        if overwrite:
            try:
                conn = self.conn or self.get_connection()
                conn.execute('drop table jsondata')
            except sqlite3.OperationalError:
                pass

        self.create_tables()

        if root_type == BOOL:
            value = 'true' if value else 'false'
        elif root_type == NUM:
            value = value

        c = self.cursor or self.get_cursor()
        conn = self.conn
        c.execute(SQL_INSERT_ROOT, (-1, root_type, value))
        conn.commit()

        return self
 
    #@timeit
    def feed(self, data, parent_id=0):
        c = self.cursor or self.get_cursor()

        parent = self.get_row(parent_id)
        parent_type = parent['type']
 
        _type = get_data_type(data)
        if _type == DICT:
            if parent_type == DICT:
                hash_id = parent_id
            else:
                c.execute(SQL_INSERT, (parent_id, _type, '',))
                hash_id = c.lastrowid
            for key, value in data.iteritems():
                c.execute(SQL_INSERT, (hash_id, KEY, key,))
                key_id = c.lastrowid
                self.feed(value, key_id)

        elif _type == LIST:
            c.execute(SQL_INSERT, (parent_id, _type, '',))
            hash_id = c.lastrowid
            for x in data:
                self.feed(x, hash_id)
        else:
            c.execute(SQL_INSERT, (parent_id, _type, data,))

        #self.conn.commit()

    @timeit
    def xpath(self, path, node_id=0):
        print 'path', path
        paths = path[2:].split('.')
        c = self.cursor or self.get_cursor()

        parent_id = node_id

        with self.conn:
            for i, name in enumerate(paths[:-1]):
                #print i, name
                row = self.get_dict_items(parent_id, name, only_one=True).next()
                if not row:
                    return []
                parent_id = row['id']
        
            #print paths[-1]
            return [(row['id'], row['value']) for row in self.get_dict_items(parent_id, value=paths[-1]) if row]

    def get_row(self, row_id):
        c = self.cursor or self.get_cursor()
        c.execute(SQL_SELECT, (row_id, ))
        rslt = c.fetchone()
        return rslt

    def get_dict_items(self, parent_id, value, only_one=False):
        c = self.cursor or self.get_cursor()
        
        extra = " LIMIT 1" if only_one else ""
        rows = c.execute(SQL_SELECT_DICT_ITEMS + extra, (parent_id, value))
        for row in rows:
            #print 'row', row
            if row['type'] == LIST:
                for item in c.execute(SQL_SELECT_CHILDREN, (row['id'],)):
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
            for row in  c:
                yield row

    def build_node(self, row):
        node = get_initial_data(row['type'])
        _type = row['type']
        #print row['id'], type(node)

        if _type == KEY:
            for child in self.get_children(row['id']):
                node = {row['value'] : self.build_node(child)}
                break

        elif _type in (LIST, DICT):
            func = node.update if _type == DICT else node.append
            for child in self.get_children(row['id']):
                #print 'add child', child
                func(self.build_node(child))

        elif _type in (STR, UNICODE):
            node = row['value']

        elif _type == BOOL:
            node = row['value'] == 'true'

        elif _type == NUM:
            node = float(row['value'])

        elif _type == NULL:
            node = None

        return node

    @classmethod
    @timeit
    def from_file(cls, dbpath, filepath):
        json = simplejson.load(open(filepath))
        _type = get_data_type(json)
        self = cls.create(dbpath, root_type=_type)
        with self.conn:
            self.feed(json)
        return self

    def dumps(self):
        root = self.get_row(0)
        return self.build_node(root)

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.commit()
            self.conn.close()


if __name__ == '__main__':
    print '-'*40
    print 'test string'
    db = JsonDB.create('foo.db', root_type=STR, value='hello world!')
    #print 'dumps', db.dumps()
    db.close()
    db = JsonDB.load('foo.db')
    assert db.dumps() == 'hello world!'

    print '-'*40
    print 'test bool'
    db = JsonDB.create('bar.db', root_type=BOOL, value=True)
    assert db.dumps() == True

    print '-'*40
    print 'test num'
    db = JsonDB.create('bar.db', root_type=NUM, value=1.2)
    assert db.dumps() == 1.2

    print '-'*40
    print'test list'
    db = JsonDB.create('bar.db', root_type=LIST)
    db.feed('hello')
    db.feed('world!')
    db.feed([1, 2])
    #print 'dumps', db.dumps()
    db.close()
    db = JsonDB.load('bar.db')
    #print db.dumps()
    assert db.dumps() == ['hello', 'world!', [1.0, 2.0]]


    print '-'*40
    print'test dict'
    db = JsonDB.create('bar.db', root_type=DICT)
    db.feed({'name': 'koba'})
    db.feed({'files': ['xxx.py', 345, None, True]})
    db.close()
    db = JsonDB.load('bar.db')
    print 'dumps', db.dumps()
    print db.xpath('$.files')

    print '-'*40
    print'test from file'
    db = JsonDB.from_file('bar.db', '1.json')
    db.close()
    db = JsonDB.load('bar.db')
    #print 'dumps', db.dumps()
    rslts = db.xpath('$.Domain')
    for _id, _name in rslts:
        print _id, _name
        print db.xpath('$.typedef.type_name', _id)

