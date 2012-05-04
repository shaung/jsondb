# -*- coding: utf-8 -*-

"""
    jsondb.backends.sqlite3backend
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

"""

import os
import sqlite3
from jsondb.backends.base import BackendBase
from jsondb.constants import *
from jsondb.jsonquery import parse


SQL_INSERT_ROOT         = "insert into jsondata values(-1, -2, ?, ?, null)"
SQL_INSERT              = "insert into jsondata values(null, ?, ?, ?, null)"
SQL_UPDATE_LINK         = "update jsondata set link = ? where id = ?"
SQL_UPDATE_VALUE        = "update jsondata set value = ? where id = ?"
SQL_SELECT_DICT_ITEMS   = "select id, type, value, link from jsondata where parent in (select distinct id from jsondata where parent = ? and type = %s and value = ?) order by id asc" % KEY
SQL_SELECT_CHILDREN     = "select id, type, value, link from jsondata where parent = ? order by id asc"
SQL_SELECT_CHILDREN_COND = "select t.id, t.type, t.value, t.link from jsondata t where t.parent = ? %s order by t.id %s %s"
SQL_SELECT = "select * from jsondata where id = ?"


class Sqlite3Backend(BackendBase):
    def __init__(self, filepath, *args, **kws):
        self.conn = None
        self.cursor = None
        self.dbpath = filepath

        overwrite = kws.get('overwrite', False)
        if overwrite or not os.path.exists(self.dbpath):
            try:
                conn = self.conn or self.get_connection()
                conn.execute('drop table jsondata')
            except sqlite3.OperationalError:
                pass

            self.create_tables()

        else:
            self.conn = self.get_connection()

        super(Sqlite3Backend, self).__init__(*args, **kws)

    def create_tables(self):
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
        self.conn = conn

    def get_connection(self, force=False):
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

            def get_ancestors(id):
                result = [id]
                while id > -2:
                    row = self.conn.execute('select parent from jsondata where id = ?', (id,)).fetchone()
                    id = row['parent']
                    result.append(id)
                return result

            self.conn.create_function('get_ancestors', 1, get_ancestors)

            def ancestors_in(id, candicates):
                # FIXME: Find a better way to do this.
                candicates = eval(candicates)
                print 'id', id, 'candicates', candicates
                while id > -2:
                    if id in candicates:
                        return True
                    row = self.conn.execute('select parent from jsondata where id = ?', (id,)).fetchone()
                    id = row['parent']
                return False

            self.conn.create_function('ancestors_in', 2, ancestors_in)

        return self.conn

    def get_cursor(self):
        if not self.cursor or not self.conn:
            conn = self.conn or self.get_connection()
            try:
                self.cursor.close()
            except:
                pass
            self.cursor= conn.cursor()
        return self.cursor

    def commit(self):
        self.conn.commit()

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.commit()
            self.conn.close()

    def insert_root(self, (root_type, value)):
        c = self.cursor or self.get_cursor()
        conn = self.conn or self.get_connection()
        c.execute(SQL_INSERT_ROOT, (root_type, value))
        conn.commit()

    def insert(self, *args, **kws):
        c = self.cursor or self.get_cursor()
        c.execute(SQL_INSERT, *args, **kws)
        return c.lastrowid

    def batch_insert(self, pending_list=[]):
        c = self.cursor or self.get_cursor()
        c.executemany(SQL_INSERT, pending_list)

    def iget_children(self, parent_id, value, only_one):
        c = self.cursor or self.get_cursor()
        sql = SQL_SELECT_CHILDREN
        paras = [parent_id]
        if value is not None:
            sql += ' and value = ? '
            paras.append(value)

        c.execute(sql, tuple(paras))
        func = only_one and c.fetchone or c.fetchall
        for row in func():
            yield row
 
    def dumprows(self):
        c = self.cursor or self.get_cursor()
        c.execute('select * from jsondata order by id')
        fmt = '{0:>12} {1:>12} {2:12} {3:12}'
        yield fmt.format('id', 'parent', 'type', 'value')
        for row in c.fetchall():
            yield fmt.format(row['id'], row['parent'], DATA_TYPE_NAME[row['type']], row['value'])

    def iget_dict_items(self, parent_id, value, cond, order, extra):
        c = self.cursor or self.get_cursor()
        
        rows = c.execute(SQL_SELECT_DICT_ITEMS + ' limit 1', (parent_id, value))
        for row in rows:
            if row['type'] == LIST:
                sql = SQL_SELECT_CHILDREN_COND % (cond, order, extra)
                for item in c.execute(sql, (row['id'],)):
                    yield item
            else:
                yield row

    def get_row(self, rowid):
        c = self.cursor or self.get_cursor()
        c.execute(SQL_SELECT, (rowid, ))
        rslt = c.fetchone()
        return rslt

    def update_link(self, rowid, link=None):
        c = self.cursor or self.get_cursor()
        c.execute('update jsondata set link = ? where id = ?', (link, rowid, ))

    def _get_hash_id(self, name):
        c = self.cursor or self.get_cursor()
        c.execute('''select max(id) as max_id from jsondata
        ''')
        max_id = c.fetchone()['max_id']
        return max_id + 1 if max_id else 1

    def set_value(self, id, value):
        c = self.cursor or self.get_cursor()
        c.execute(SQL_UPDATE_VALUE, (value, id))

    def select(self, stmt, variables=()):
        print stmt, variables
        c = self.cursor or self.get_cursor()
        c.execute(stmt, variables)
        return c.fetchall()

    def jsonpath(self, path, parent=-1, one=False):
        parent_ids = [parent]
        funcs = {
            'predicate' : parse_predicate,
            'union'     : parse_union,
        }

        ast = parse(path)
        sqlstmt = ''
        for idx, node in enumerate(ast['jsonpath']):
            is_last = (idx == len(ast['jsonpath']) - 1)
            if is_last:
                select_cols = 'select rowid as rowno, t.id as id, t.parent as parent, t.type as type, t.value as value, t.link as link from jsondata t'
            else:
                select_cols = 'select rowid as rowno, t.id as id, t.parent as parent, t.type as type from jsondata t'

            # The approach here:
            # Select the rows based on name / axis.
            # Then apply filters to make the id set smaller.
            # Then select against their children.

            print node
            tag = node['tag']
            name = tag.get('name', '')
            axis = tag.get('axis', '.')
            # if name and axis
            # Now we are selecting rows that could become the ancient parents of the final results.
            # If axis == '.', then we only need to find t where t.parent in (CURRENT_ID_SET)
            # If axis == '..', then we must find all possible children.
            #    If name == '', then we just need to find the direct chidren.
            #    Otherwise, we need to find all children.
            # If name == '', then we are selecting against all chidlren of every type.
            # If predicate is provided, just append it to the where clause.
            # If union is provided, they go to the limit/offset/orderby clause.
            #    note: merge the result in python, not the sql.
            if axis == '.':
                if name:
                    rows = self.select(select_cols + ' where t.parent in (select distinct id from jsondata where parent in (%s) and type = %s and value = "%s") order by id asc' % (','.join(map(str, parent_ids)), KEY, name))
                else:
                    # TODO: "$.*.author"
                    rows = []
            elif axis == '..':
                if name:
                    # We are looking for DICTS who has a key named "name"
                    rows = self.select(select_cols + ' where t.parent in '
                                                    '(select id from jsondata tk where tk.type = ? and tk.value = ? and '
                                                    'exists(select id from jsondata tp where tp.type = ? and tp.id = tk.parent and ancestors_in(tp.parent, ?)))', (KEY, name, DICT, repr(parent_ids),))
                else:
                    # ..* is meaningless.
                    # TODO: just ignore it for now.
                    rows = []

            def expand(row):
                if row['type'] == LIST:
                    return self.select(select_cols + ' where t.parent = %s' % row['id'])
                else:
                    return [row]
            rows = sum((expand(row) for row in rows), [])

            rowids = [row['id'] for row in rows]
            print 'rowids', rowids
            if not rowids:
                # No matches
                break

            for _filter in node.get('filter_list', []):
                func = funcs.get(_filter['type'])
                rowids = func(_filter, rowids)
            parent_ids = rowids

        for row in rows:
            print 'row', row, row.keys(), [x for x in row]
            yield Result(row['id'], row['value'] if row['type'] != BOOL else bool(row['value']), row['link'])
            if one: break


def parse_predicate(_filter, rowids):
    result = rowids
    # The parent is a LIST or DICT
    # when LIST, the condition applies to each of it's children
    # when DICT, applies to itself
    return result


def parse_union(_filter, rowids):
    result = []
    for union in _filter['value']:
        _type = union['type']
        if _type == 'index':
            index = int(union['value'])
            try:
                if index not in rowids:
                    result.append(rowids[index])
            except IndexError:
                pass
        elif _type == 'slicing':
            start = union.get('start', None)
            end = union.get('end', None)
            step = union.get('step', None)
            _slice = [(int(x) if x else None) for x in (start, end, step)]
            result += [id for id in rowids[slice(*_slice)] if id not in result]
 
    return result

