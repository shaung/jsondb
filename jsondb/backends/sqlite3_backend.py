# -*- coding: utf-8 -*-

"""
    jsondb.backends.sqlite3backend
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    Sqlite3 backend for jsondb

"""

import os
import re
import sqlite3

from jsondb.backends.base import BackendBase
from jsondb.datatypes import *

import logging
logger = logging.getLogger(__file__)

SQL_INSERT_ROOT     = "insert into jsondata values(-1, -2, ?, ?, null)"
SQL_INSERT          = "insert into jsondata values(null, ?, ?, ?, null)"
SQL_UPDATE_LINK     = "update jsondata set link = ? where id = ?"
SQL_UPDATE_VALUE    = "update jsondata set value = ? where id = ?"
SQL_SELECT_CHILDREN = "select id, type, value, link from jsondata where parent = ? order by id asc"
SQL_SELECT          = "select * from jsondata where id = ?"


class Sqlite3Backend(BackendBase):
    def __init__(self, url, *args, **kws):
        self.conn = None
        self.cursor = None
        self.url = url
        self.dbpath = url.database
        self.link_key = kws.get('link_key')

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

    def get_path(self):
        return os.path.normpath(self.dbpath)

    def get_url(self):
        return unicode(self.url)

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

        conn.execute("""create table if not exists settings
        (key    text primary key,
         value  blob
        )""")

        conn.execute("insert or replace into settings(key, value) values(?, ?)", ('link_key', self.link_key))

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

            def ancestors_in(id, candicates):
                # FIXME: Find a better way to do this.
                candicates = eval(candicates)
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
            self.cursor = conn.cursor()
        return self.cursor

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.commit()
            self.conn.close()

    def update_settings(self, key, value):
        conn = self.conn or self.get_connection()
        conn.execute('update settings set value = ? where key = ?', (key, value))
        conn.commit()

    def get_settings(self, key):
        c = self.cursor or self.get_cursor()
        c.execute('select value from settings where key = ?', (key,))
        rslt = c.fetchone()
        return rslt['value']

    def get_root_type(self):
        c = self.cursor or self.get_cursor()
        c.execute('select type from jsondata where id = ?', (-1,))
        rslt = c.fetchone()
        return rslt['type']

    def find_key(self, key, parent_id):
        c = self.cursor or self.get_cursor()
        c.execute('select id from jsondata where type = ? and value = ? and parent = ?', (KEY, key, parent_id))
        rslt = c.fetchone()
        key_id = rslt['id'] if rslt else None
        if key_id is None:
            return None, None

        c.execute('select id from jsondata where parent = ?', (key_id,))
        rslt = c.fetchone()
        return key_id, rslt['id'] if rslt else None

    def get_nth_child(self, parent_id, offset):
        c = self.cursor or self.get_cursor()
        if offset >= 0:
            order_clause = 'order by id limit 1 offset ?'
        else:
            offset = offset * -1 - 1
            order_clause = 'order by id desc limit 1 offset ?'
        c.execute('select rowid as rowno, id, parent, type, link from jsondata where parent = ? %s' % order_clause, (parent_id, offset))
        rslt = c.fetchone()
        return Result.from_row(rslt)

    def iter_slice(self, id, start=None, stop=None, step=None):
        c = self.cursor or self.get_cursor()
        c.execute('select id, type from jsondata where parent = ? order by id', (id,))
        s = slice(start, stop, step)
        rowids = (row['id'] for row in c.fetchall())
        if start < 0 or stop < 0 or step < 0:
            result = list(rowids)[s]
        else:
            result = islice(rowids, start, stop, step)
        return result

    def iter_dict(self, parent_id):
        c = self.cursor or self.get_cursor()
        c.execute('select id, value from jsondata where type = ? and parent = ?', (KEY, parent_id))
        row_keys = c.fetchall()
        for row_key in row_keys:
            key = row_key['value']
            c.execute('select id, type, value, link from jsondata where parent = ?', (row_key['id'],))
            row = c.fetchone()
            value_id = row['id']
            yield key, Result.from_row(row)

    def remove(self, id, recursive=True, include_self=False):
        c = self.cursor or self.get_cursor()
        if recursive:
            c.execute('delete from jsondata where ancestors_in(parent, ?)', (repr((id,)),))
        else:
            c.execute('delete from jsondata where parent = ?', (id,))
        if include_self:
            c.execute('delete from jsondata where id = ?', (id,))

    def set_link_key(self, key):
        self.update_settings('link_key', key)

    def get_link_key(self):
        return self.get_settings('link_key')

    def insert_root(self, (root_type, value)):
        c = self.cursor or self.get_cursor()
        conn = self.conn or self.get_connection()
        c.execute(SQL_INSERT_ROOT, (root_type, value))
        conn.commit()

    def set_row(self, id, type, value):
        c = self.cursor or self.get_cursor()
        conn = self.conn or self.get_connection()
        c.execute('update jsondata set type = ?, value = ? where id = ?', (type, value, id))
        conn.commit()

    def insert(self, *args, **kws):
        c = self.cursor or self.get_cursor()
        c.execute(SQL_INSERT, *args, **kws)
        return c.lastrowid

    def batch_insert(self, pending_list=[]):
        c = self.cursor or self.get_cursor()
        c.executemany(SQL_INSERT, pending_list)

    def iter_children(self, parent_id, value=None, only_one=False):
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
            yield fmt.format(row['id'], row['parent'], DATA_TYPE_NAME[row['type']], 'LINK: %s' % row['link'] if row['link'] else row['value'])

    def get_row(self, rowid):
        c = self.cursor or self.get_cursor()
        c.execute(SQL_SELECT, (rowid, ))
        rslt = c.fetchone()
        return rslt

    def get_row_type(self, rowid):
        c = self.cursor or self.get_cursor()
        c.execute('select type from jsondata where id = ?', (rowid, ))
        rslt = c.fetchone()
        return rslt['type'] if rslt else None

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

    def get_children_count(self, id):
        c = self.cursor or self.get_cursor()
        c.execute("select count(id) as count from jsondata where parent = ?", (id,))
        result = c.fetchone()
        return result['count']

    def increase_value(self, id, increase_by=0):
        c = self.cursor or self.get_cursor()
        c.execute("update jsondata set value = value + ? where id = ?", (increase_by, id))

    def select(self, stmt, variables=()):
        c = self.cursor or self.get_cursor()
        c.execute(stmt, variables)
        result = c.fetchall()
        return result

    def jsonpath(self, ast, parent=-1, one=False):
        parent_ids = [parent]
        parent_types = [self.get_row_type(id) for id in parent_ids]
        funcs = {
            'predicate' : self.parse_predicate,
            'union'     : self.parse_union,
        }

        sqlstmt = ''
        for idx, node in enumerate(ast['jsonpath']):
            is_last = (idx == len(ast['jsonpath']) - 1)
            if is_last:
                select_cols = 'select rowid as rowno, t.id as id, t.parent as parent, t.type as type, t.link as link from jsondata t'
            else:
                select_cols = 'select rowid as rowno, t.id as id, t.parent as parent, t.type as type from jsondata t'

            # The approach here:
            # Select the rows based on name / axis.
            # Then apply filters to make the id set smaller.
            # Then select against their children.

            tag = node['tag']
            name = tag.get('name', '')
            axis = tag.get('axis', '.')
            filters = node.get('filter_list', [])
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
                    # Expand lists
                    for i, parent_type in enumerate(reversed(parent_types)):
                        if parent_type == LIST:
                            parent_ids[i:i + 1] = self.iter_slice(parent_ids[i])
                    if not parent_ids:
                        rows = []
                    else:
                        rows = self.select('%s where t.parent in ('
                                           'select distinct id from jsondata'
                                           ' where parent in (%s) and type = %s and value = \'%s\')'
                                           ' order by id asc' %
                                           (select_cols, ','.join(map(str, parent_ids)), KEY, name))
                else:
                    # "$.*.author"
                    rows = self.select(
                        '%s where t.parent in (%s)'
                        ' order by id asc' %
                        (select_cols, ','.join(map(str, parent_ids))))
                    # for dict keys, should fetch all the value nodes
                    rows = sum([
                        tuple(self.select(
                            '%s where t.parent = %s'
                            ' order by id asc' %
                            (select_cols, row['id'])))
                        if row['type'] == KEY
                        else tuple([row])
                        for row in rows], ())
            elif axis == '..':
                if name:
                    # We are looking for DICTS who has a key named "name"
                    rows = self.select('%s where t.parent in ('
                                       'select id from jsondata tk'
                                       ' where tk.type = ? and tk.value = ? and '
                                       ' exists (select id from jsondata tp'
                                       ' where tp.type = ? and tp.id = tk.parent and ancestors_in(tp.parent, ?)))' % (select_cols,),
                                       (KEY, name, DICT, repr(parent_ids),))
                else:
                    # ..* is meaningless.
                    # TODO: just ignore it for now.
                    rows = []

            def expand(row):
                if row['type'] == LIST:
                    return self.select(select_cols + ' where t.parent = %s' % row['id'])
                else:
                    return [row]

            if not is_last or filters:
                rows = sum((expand(row) for row in rows), [])

            rowids = [row['id'] for row in rows]
            if not rowids:
                # No matches
                break

            for _filter in filters:
                func = funcs.get(_filter['type'])
                rowids = func(_filter, rowids)

            if is_last:
                rows = [r for r in rows if r['id'] in rowids]
            else:
                parent_ids = rowids
                parent_types = [r['type'] for r in rows if r['id'] in rowids]

        for row in rows:
            yield Result.from_row(row)
            if one:
                break

    def parse_predicate(self, _filter, rowids):
        # Evaluate the expr
        # The parent is a LIST or DICT
        # when LIST, the condition applies to each of it's children
        # when DICT, applies to itself

        result = rowids
        parse_atom.children = {}
        expr = _filter['expr']
        _type, condition = parse_expr(expr)
        if _type == 'child':
            condition += ' is not NULL '

        stmt = ''
        tables = {}
        for key, childnodes in parse_atom.children.items():
            # TODO: Check the child exists and passes the condition
            condition = re.sub(key, '%s.type >= 0 and %s.value' % (key, key), condition)
            subquery = ''
            for i, node in enumerate(childnodes):
                is_last = (i == len(childnodes) - 1)
                if is_last:
                    cols = 'id, type, value'
                else:
                    cols = 'id'
                tag = node['tag']
                name = tag.get('name', '')
                axis = tag.get('axis', '.')
                alias = '%s%s' % (key, i)
                if name and axis == '.':
                    _query = 'select %%s from jsondata where parent = (select id from jsondata where type = %s and parent = %%s and value = %%s)' % KEY
                    if not subquery:
                        parent = 't.id'
                    else:
                        parent = '(%s)' % subquery
                    subquery = _query % (cols, parent, "'%s'" % name)
                else:
                    # TODO
                    pass
            subquery += ' union all select -9, -1, NULL'
            tables[key] = subquery

        clause = 'select %s from %s where (%s) and (%s)' % ('*', ', '.join('(%s) %s' % (v, k) for k, v in tables.items()), condition,
                    ' or '.join(['%s.type >= 0' % k for k in tables]))
        stmt = 'select t.id from jsondata t where t.id in (%s) and exists (%s)' % (','.join(str(x) for x in rowids), clause)

        rows = self.select(stmt)
        result = [row['id'] for row in rows]

        return result

    def parse_union(self, _filter, rowids):
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


def parse_atom(atom):
    _type = atom.get('type')
    _value = atom.get('value')
    if _type in ('number', 'literal'):
        return _type, atom.get('value')
    elif _type == 'boolean':
        return _type, _value == 'True' and 1 or 0
    elif _type == 'child':
        key = '__t%s__' % len(parse_atom.children)
        parse_atom.children[key] = _value
        return _type, key

    elif _type == 'func':
        # TODO:
        return _type, ''
    elif _type == 'expr':
        return _type, ' (%s) ' % parse_expr(_value)[-1]
    else:
        raise 'impossible'

parse_atom.children = {}


def parse_expr(expr):
    result = ''

    if not expr:
        return None, ''

    _type = expr.get('type')
    if not _type:
        if 'atom' in expr:
            atom = expr.get('atom')
            return parse_atom(atom)
        elif 'expr_list' in expr:
            result = ' (%s)' % ','.join((parse_expr(x['expr'])[-1] for x in expr.get('expr_list', [])))

    else:
        left = expr.get('left')
        right = expr.get('right')
        op = expr.get('op')
        fmt = '%s %s %s'
        if op == 'or':
            fmt = '(%s) %s (%s)'
        # FIXME: It seems "=" does not work properly in some environments.
        #        Dont know the reason yet. Just make use of the sqlite3 syntax sugar for now.
        if op in ('=', '=='):
            op = 'is'
        lexprs = parse_expr(left)
        if lexprs[0] == 'child' and op in ('and', 'or', 'not'):
            lexpr = ' %s is not NULL ' % lexprs[1]
        else:
            lexpr = lexprs[1]
        rexprs = parse_expr(right)
        if rexprs[0] == 'child' and op in ('and', 'or', 'not'):
            rexpr = ' %s is not NULL ' % rexprs[1]
        else:
            rexpr = rexprs[1]

        result = fmt % (lexpr or 1, op if lexpr else ' and %s ' % op , rexpr)
        _type = op

    return _type, result
