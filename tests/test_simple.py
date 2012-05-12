# -*- coding: utf-8 -*-

"""
    jsondb.tests
    ~~~~~~~~~~~~

    Tests for jsondb.
"""

import os
from jsondb import *
from nose.tools import eq_

class TestSimpleTypes:
    def test_string(self):
        self.eq(STR, 'Hello world!')
        self.eq(UNICODE, u'Hello world!')

    def test_bool(self):
        self.eq(BOOL, True)
        self.eq(BOOL, False)
        self.eq(BOOL, 1)
        self.eq(BOOL, 0)

    def test_int(self):
        for i in xrange(100):
            self.eq(INT, i)

    def test_float(self):
        self.eq(FLOAT, 1.2)
        self.eq(FLOAT, 0.99999)
        self.eq(FLOAT, 0.00000000000000000001)

    def test_nil(self):
        self.eq(NIL, None)

    def eq(self, root_type, value):
        db = JsonDB.create(root_type=root_type, value=value)
        dbpath = db.get_path()
        db.close()
        db = JsonDB.load(dbpath)
        eq_(db.dumps(), value)

 
def test_list():
    """test list"""
    db = JsonDB.create('bar.db', root_type=LIST)
    db.feed('hello')
    db.feed('world!')
    db.feed([1, 2])
    db.close()
    db = JsonDB.load('bar.db')
    eq_(db.dumps(), ['hello', 'world!', [1.0, 2.0]])

def test_list_merge():
    """merge into a list"""

    data = ['initial item', 'added item1', 'item 2', 'item3-key']

    db = JsonDB.create('bar.db', root_type=DICT)
    _list_id = db.feed({'root' : data[:1]})[0]
    db.feed(data[1], _list_id)
    for x in data[2:]:
        db.feed(x, _list_id)
    db.close()

    db = JsonDB.load('bar.db')

    db.dumprows()
    rslt = [x.value for x in db.query('$.root')]
    eq_(rslt, data)

def test_dict():
    """test dict"""
    db = JsonDB.create('bar.db', root_type=DICT)
    files = ['xxx.py', 345, None, True, 'wtf', {'foo' : ['f1', 'f2']}]
    _id = db.feed({'name': []})[0]
    db.feed({'files': files}, _id)
    db.feed({
        'bloon': "here you ARE!",
        'crazy': '2'}, _id)
    db.feed({
        'bloon': "well!",
        'crazy': '4'}, _id)

    h_dom = db.feed({'dom' : []})[0]
    db.feed({'love': 1}, h_dom)
    db.close()
    db = JsonDB.load('bar.db')
    for i in range(len(files)):
        for rslt in db.query('$.name[-1].files[%s]' % i):
            assert rslt.value in files

    rslt = [x.value for x in db.query('$.name[?(@.crazy in ("2", "4"))].bloon')]
    eq_(rslt, ['here you ARE!', 'well!'])

    rslt = [x.value for x in db.query('$.name[?(@.crazy = "2")].bloon')]
    eq_(rslt, ['here you ARE!'])

    rslt = [x.value for x in db.query('$.name[-1:].bloon')]
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
            self.db = JsonDB.from_file(self.dbpath, 'bookstore.json')
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
        rslt = list(self.db.query(path).values())
        eq_(rslt, expected)
 

def test_large():
    db = JsonDB.create('large.db', root_type=DICT)
    for i in range(1000):
        li = db.feed({str(i):{'value':str(i)}})

    #db.dump('large.json')
    db.close()

    db = JsonDB.load('large.db')
    rslt = db.query('$.15.value').getone().value
    eq_(rslt, str(15))

if __name__ == '__main__':
    pass
