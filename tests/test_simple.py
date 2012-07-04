# -*- coding: utf-8 -*-

"""
    jsondb.tests
    ~~~~~~~~~~~~

    Tests for jsondb.
"""

import os
from jsondb import *
from nose.tools import eq_

import logging
logging.basicConfig(level='DEBUG')


class TestBase:
    def eq_dumps(self, root_type, value):
        db = JsonDB.create(root_type=root_type, value=value)
        dbpath = db.get_path()
        db.close()
        db = JsonDB.load(dbpath)
        eq_(db.dumps(), value)


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
        db = JsonDB.create(value=data)
        db.close()
        dbpath = db.get_path()
        db = JsonDB.load(dbpath)
        eq_(db.dumps(), data)

    def test_list(self):
        """test list"""
        data = ['hello', 'world!', [1, 2.0]]
        db = JsonDB.create(value=[])
        for x in data:
            db.feed(x)
        db.close()
        dbpath = db.get_path()
        db = JsonDB.load(dbpath)
        eq_(db.dumps(), data)

    def test_list_merge(self):
        """merge into a list"""

        data = ['initial item', 'added item1', 'item 2', 'item3-key']

        db = JsonDB.create(value={})
        _list_id = db.feed({'root' : data[:1]})[0]
        db.feed(data[1], _list_id)
        for x in data[2:]:
            db.feed(x, _list_id)
        db.close()
        dbpath = db.get_path()

        db = JsonDB.load(dbpath)

        db.dumprows()
        path = '$.root'
        rslt = list(db.query('$.root').values())
        eq_(rslt, data)


class TestDicts:
    def test_dict(self):
        """test dict"""
        db = JsonDB.create(root_type=DICT)
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

        db = JsonDB.load(dbpath)

        for i in range(len(files)):
            for rslt in db.query('$.name[-1].files[%s]' % i):
                assert rslt.value in files

        path = '$.name[?(@.crazy in ("2", "4"))].bloon'
        rslt = list(db.query(path).values())
        eq_(rslt, ['type', 'well!'])

        path = '$.name[?(@.crazy = "2")].bloon'
        rslt = list(db.query(path).values())
        eq_(rslt, ['type'])

        path = '$.name[-1:].bloon'
        rslt = list(db.query(path).values())
        eq_(rslt, ['well!'])


class TestBookStore:
    all_titles = ['Sayings of the Century', 'Sword of Honour', 'Moby Dick', 'The Lord of the Rings']
    all_authors = ['Nigel Rees', 'Evelyn Waugh', 'Herman Melville', 'J. R. R. Tolkien']
    all_prices = [8.95, 12.99, 8.99, 22.99, 19.95]

    def setup(self):
        self.dbpath = 'bookstore.db'
        if os.path.exists(self.dbpath):
            self.db = JsonDB.load(self.dbpath)
        else:
            fpath = os.path.join(os.path.dirname(__file__), 'bookstore.json')
            self.db = JsonDB.from_file(self.dbpath, fpath)
        self.db.dumprows()

    def teardown(self):
        self.db.close()

    def test_condition(self):
        path = '$.store.book[?(@.author="Evelyn Waugh")].title'
        self.eq(path, ['Sword of Honour'])

    def test_query_position(self):
        path = '$.store.book[0].title'
        self.eq(path, self.all_titles[0:1])
        path = '$.store.book[2].title'
        self.eq(path, self.all_titles[2:3])

    def test_query_range_all(self):
        path = '$.store.book.title'
        self.eq(path, self.all_titles)
        path = '$.store.book[*].title'
        self.eq(path, self.all_titles)
        path = '$.store.book[*].author'
        self.eq(path, self.all_authors)
 
    def test_query_range_pm(self):
        path = '$.store.book[1:-1].author'
        self.eq(path, self.all_authors[1:-1])

    def test_query_range_pn(self):
        path = '$.store.book[2:].author'
        self.eq(path, self.all_authors[2:])

    def test_query_range_mn(self):
        path = '$.store.book[-2:].author'
        self.eq(path, self.all_authors[-2:])

    def test_query_range_nm(self):
        path = '$.store.book[:-1].author'
        self.eq(path, self.all_authors[:-1])
        path = '$.store.book[:-2].author'
        self.eq(path, self.all_authors[:-2])

    def test_query_range_mm(self):
        path = '$.store.book[-2:-1].author'
        self.eq(path, self.all_authors[-2:-1])
        path = '$.store.book[-9:-2].author'
        self.eq(path, self.all_authors[-9:-2])

    def test_query_range_union(self):
        path = '$.store.book[1, 2, -1].author'
        self.eq(path, self.all_authors[1:3] + self.all_authors[-1:])
        path = '$.store.book[0:1, -2].author'
        self.eq(path, self.all_authors[0:1] + self.all_authors[-2:-1])

    def test_query_slicing_step(self):
        path = '$.store.book[-4:-1:-1].author'
        self.eq(path, self.all_authors[-4:-1:-1])
        path = '$.store.book[-4:-1:-2].author'
        self.eq(path, self.all_authors[-4:-1:-2])

    def test_query_recursive_descent(self):
        path = '$.store..price'
        self.eq(path, self.all_prices)
        path = '$..price'
        self.eq(path, self.all_prices)

    def test_query_exists(self):
        path = '$.store.book[?(@.isbn)].author'
        self.eq(path, self.all_authors[-2:])

    def eq(self, path, expected):
        rslt = self.db.query(path).values()
        eq_(rslt, expected)
 
class TestCreate(TestBookStore):
    def setup(self):
        self.dbpath = 'bookstore.db'
        import json
        fpath = os.path.join(os.path.dirname(__file__), 'bookstore.json')
        data = json.load(open(fpath))
        self.db = JsonDB.create(value=data)

def test_cxt():
    with JsonDB.create(value={'name':'foo'}) as db:
        eq_(db['$.name'].getone().value, 'foo')
        eq_(db[1].value, 'foo')
        try:
            db[True]
        except UnsupportedOperation:
            pass


def test_large():
    db = JsonDB.create('large.db', root_type=DICT)
    for i in range(1000):
        li = db.feed({str(i):{'value':str(i)}})

    db.close()

    db = JsonDB.load('large.db')
    rslt = db.query('$.15.value').getone().value
    eq_(rslt, str(15))


def test_composed():
    obj = {'Obj':[
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

    db = JsonDB.create(value=obj)

    db.dumprows()

    rslt = db.query('$.Obj.name').values()
    eq_(rslt, [obj['Obj'][0]['name'], obj['Obj'][1]['name']])

    rslt = db.query('$.Obj.parenta').values()
    eq_(rslt, [obj['Obj'][0]['parenta'], obj['Obj'][1]['parenta']])

    rslt = db.query('$.Obj.domain').values()
    eq_(rslt, [obj['Obj'][0]['domain'], obj['Obj'][1]['domain']])

    rslt = db.query('$.Obj.type').values()
    eq_(rslt, [obj['Obj'][0]['type'], obj['Obj'][1]['type']])


    rslt = db.query('$.Obj.parent').values()
    eq_(rslt, [obj['Obj'][0]['parent'], obj['Obj'][1]['parent']])

    rslt = db.query('$.Obj[?(@.name == "bar")].name').getone()
    eq_(rslt.value, 'bar')

    rslt = db.query('$.Obj[?(@.name == "bar")]').getone()
    eq_(rslt.id, 11)

    rslt = db.query('$.Obj.shadow')
    eq_([db.query(x.link).getone().value for x in rslt], [obj['Obj'][1]['description'], obj['Obj'][0]['description']])

    """
    rslt = db.query('$.Obj[?(@.name == "bar")]').getone()
    eq_(rslt.query('$.name').getone().value, 'bar')
    """



if __name__ == '__main__':
    pass
