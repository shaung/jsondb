# -*- coding: utf-8 -*-

"""
    jsondb.tests
    ~~~~~~~~~~~~

    Tests for jsondb.
"""

import os, json
import jsondb
from jsondb.datatypes import *
from nose.tools import eq_

import logging
logging.basicConfig(level='DEBUG')

logger = logging.getLogger(__file__)


import sqlite3
logger.debug('sqlite3 version = %s' % sqlite3.sqlite_version)


class TestBase:
    def eq_dumps(self, root_type, data):
        db = jsondb.create(data)
        dbpath = db.get_path()
        dburl = db.get_url()
        db.close()
        db = jsondb.load(dburl)
        eq_(db.dumps(), json.dumps(data))
        eq_(db.data(), data)


class TestSimpleTypes(TestBase):
    def test_string(self):
        self.eq_dumps(STR, 'Hello world!')
        self.eq_dumps(UNICODE, u'Hello world!')
        self.eq_dumps(STR, 'type')
        self.eq_dumps(STR, 'parent')
        self.eq_dumps(UNICODE, u'TYPE')

    def test_bool(self):
        self.eq_dumps(BOOL, True)
        self.eq_dumps(BOOL, False)
        self.eq_dumps(BOOL, 1)
        self.eq_dumps(BOOL, 0)

    def test_int(self):
        for i in xrange(100):
            self.eq_dumps(INT, i)

    def test_float(self):
        self.eq_dumps(FLOAT, 1.2)
        self.eq_dumps(FLOAT, 0.99999)
        self.eq_dumps(FLOAT, 0.00000000000000000001)

    def test_nil(self):
        self.eq_dumps(NIL, None)


class TestLists(TestBase):
    def test_list_create(self):
        """test list"""
        data = ['hello', 'world!', [1, 2.0]]
        db = jsondb.create(data)
        db.close()
        dbpath = db.get_path()
        db = jsondb.load(dbpath)
        eq_(db.data(), data)

    def test_list(self):
        """test list"""
        data = ['hello', 'world!', [1, 2.0]]
        db = jsondb.create([])
        for x in data:
            db.feed(x)
        db.close()
        dbpath = db.get_path()
        db = jsondb.load(dbpath)
        eq_(db.data(), data)

    def test_list_merge(self):
        """merge into a list"""

        data = ['initial item', 'added item1', 'item 2', 'item3-key']

        db = jsondb.create({})
        _list_id = db.feed({'root' : data[:1]})[0]
        db.feed(data[1], _list_id)
        for x in data[2:]:
            db.feed(x, _list_id)
        db.close()
        dbpath = db.get_path()

        db = jsondb.load(dbpath)

        path = '$.root'
        rslt = db.query('$.root').values()
        eq_(rslt, [data])


class TestDicts:
    def setup(self):
        """test dict"""
        db = jsondb.create({})
        files = ['xxx.py', 345, None, True, 'wtf', {'foo' : ['f1', 'f2']}]
        _id = db.feed({'name': []})[0]
        db.feed({'files': files}, _id)
        db.feed({
            'bloon': "type",
            'crazy': '2'}, _id)
        db.feed({
            'bloon': "well!",
            'crazy': '4'}, _id)

        h_dom = db.feed({'dom' : []})[0]
        db.feed({'love': 1}, h_dom)
        db.close()
        dbpath = db.get_path()

        self.db = jsondb.load(dbpath)
        self.files = files

    def test_dict(self):
        for i in range(len(self.files)):
            for rslt in self.db.query('$.name[-1].files[%s]' % i):
                assert rslt.value in self.files

    def test_query_in(self):
        path = '$.name[?(@.crazy in ("2", "4"))].bloon'
        rslt = list(self.db.query(path).values())
        eq_(rslt, ['type', 'well!'])

    def test_query_eq(self):
        path = '$.name[?(@.crazy = "2")].bloon'
        rslt = list(self.db.query(path).values())
        eq_(rslt, ['type'])

    def test_query_range(self):
        path = '$.name[-1:].bloon'
        rslt = list(self.db.query(path).values())
        eq_(rslt, ['well!'])


def test_reload():
    db = jsondb.create({}, url='/tmp/test.db')
    eq_(db.keys(), [])
    db['a'] = 1
    eq_(db.keys(), ['a'])
    db.close()

    db = jsondb.load(url='/tmp/test.db')
    eq_(db.keys(), ['a'])
    db.close()


def test_cxt():
    with jsondb.create({'name':'foo'}) as db:
        eq_(db['$.name'].data(), 'foo')
        try:
            db[True]
        except jsondb.UnsupportedOperation:
            pass


def test_large():
    db = jsondb.create(url='large.db')
    for i in range(1000):
        li = db.feed({str(i):{'value':str(i)}})

    db.close()

    db = jsondb.load('large.db')
    rslt = db.query('$.15.value').getone().data()
    eq_(rslt, str(15))


class TestComposed:
    def setup(self):
        self.obj = {'Obj':[
            {
                'name': 'foo',
                'description': 'FOO',
                'parenta': 'foo.parent',
                'parent': 'foo.parent',
                'type': 'a',
                'domain': 'a',
                'shadow': {
                    '@__link__': '$.Obj[?(@.name == "bar")].description',
                }
            },
            {
                'name': 'bar',
                'description': 'BAR',
                'parenta': 'bar.parent',
                'parent': 'bar.parent',
                'type': 'b',
                'domain': 'b',
                'shadow': {
                    '@__link__': '$.Obj[?(@.name == "foo")].description',
                }
            }

        ]}

        self.db = jsondb.create(self.obj)

    def eq(self, path, expected):
        rslt = self.db.query(path).values()
        eq_(rslt, expected)

    def test_name(self):
        path = '$.Obj.name'
        expected = [self.obj['Obj'][0]['name'], self.obj['Obj'][1]['name']]
        self.eq(path, expected)

    def test_parenta(self):
        path = '$.Obj.parenta'
        expected = [self.obj['Obj'][0]['parenta'], self.obj['Obj'][1]['parenta']]
        self.eq(path, expected)

    def test_domain(self):
        path = '$.Obj.domain'
        expected = [self.obj['Obj'][0]['domain'], self.obj['Obj'][1]['domain']]
        self.eq(path, expected)

    def test_type(self):
        path = '$.Obj.type'
        self.eq(path, [self.obj['Obj'][0]['type'], self.obj['Obj'][1]['type']])

    def test_parent(self):
        path = '$.Obj.parent'
        expected = [self.obj['Obj'][0]['parent'], self.obj['Obj'][1]['parent']]
        self.eq(path, expected)

    def test_value(self):
        rslt = self.db.query('$.Obj[?(@.name == "bar")].name').getone()
        eq_(rslt.data(), 'bar')

    def test_id(self):
        rslt = self.db.query('$.Obj[?(@.name == "bar")]').getone()
        eq_(rslt.id(), 11)

    def test_link(self):
        rslt = self.db.query('$.Obj.shadow')
        eq_([self.db.query(x.link()).getone().data() for x in rslt], [self.obj['Obj'][1]['description'], self.obj['Obj'][0]['description']])

    def test_experiment(self):
        pass
        """
        rslt = db.query('$.Obj[?(@.name == "bar")]').getone()
        eq_(rslt.query('$.name').getone().value, 'bar')
        """



if __name__ == '__main__':
    pass
