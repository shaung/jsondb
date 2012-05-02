# -*- coding: utf-8 -*-

"""
    jsondb.tests
    ~~~~~~~~~~~~

    Tests for jsondb.
"""

import os
from jsondb import *

def test_string():
    """test string"""
    db = JsonDB.create('foo.db', root_type=STR, value='hello world!')
    db.close()
    db = JsonDB.load('foo.db')
    assert db.dumps() == 'hello world!'

def test_bool():
    """test bool"""
    db = JsonDB.create('bar.db', root_type=BOOL, value=True)
    assert db.dumps() == True

def test_float():
    """test float"""
    db = JsonDB.create('bar.db', root_type=FLOAT, value=1.2)
    assert db.dumps() == 1.2

def test_list():
    """test list"""
    db = JsonDB.create('bar.db', root_type=LIST)
    db.feed('hello')
    db.feed('world!')
    db.feed([1, 2])
    db.close()
    db = JsonDB.load('bar.db')
    print db.dumps()
    assert db.dumps() == ['hello', 'world!', [1.0, 2.0]]

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

    rslt = [x.value for x in db.xpath('$.root')]
    assert rslt == data

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
        for rslt in db.xpath('$.name[-1].files[%s]' % i):
            assert rslt.value in files

    rslt = [x.value for x in db.xpath('$.name[?(@.crazy in ("2", "4"))].bloon')]
    assert rslt == ['here you ARE!', 'well!']

    rslt = [x.value for x in db.xpath('$.name[?(@.crazy = "2")].bloon')]
    assert rslt == ['here you ARE!']

    rslt = [x.value for x in db.xpath('$.name[-1:].bloon')]
    print rslt
    assert rslt == ['well!']


class TestQuery:
    all_titles = ['Sayings of the Century', 'Sword of Honour', 'Moby Dick', 'The Lord of the Rings']
    all_authors = ['Nigel Rees', 'Evelyn Waugh', 'Herman Melville', 'J. R. R. Tolkien']
    all_prices = [8.95, 12.99, 8.99, 22.99, 19.95]

    def setup(self):
        self.dbpath = 'bookstore.db'
        if os.path.exists(self.dbpath):
            self.db = JsonDB.load(self.dbpath)
        else:
            self.db = JsonDB.from_file(self.dbpath, 'bookstore.json')

    def teardown(self):
        self.db.close()

    def test_condition(self):
        rslt = [x.value for x in self.db.xpath('$.store.book[?(@.author="Evelyn Waugh")].title')]
        assert rslt == ['Sword of Honour']

    def test_query_position(self):
        rslt = [x.value for x in self.db.xpath('$.store.book[0].title')]
        assert rslt == self.all_titles[0:1]
        rslt = [x.value for x in self.db.xpath('$.store.book[2].title')]
        assert rslt == self.all_titles[2:3]

    def test_query_range_all(self):
        rslt = [x.value for x in self.db.xpath('$.store.book.title')]
        assert rslt == self.all_titles
        rslt = [x.value for x in self.db.xpath('$.store.book[*].title')]
        assert rslt == self.all_titles
        rslt = [x.value for x in self.db.xpath('$.store.book[*].author')]
        assert rslt == self.all_authors
 
    def test_query_range_pm(self):
        rslt = [x.value for x in self.db.xpath('$.store.book[1:-1].author')]
        print rslt
        assert rslt == self.all_authors[1:-1]

    def test_query_range_pn(self):
        rslt = [x.value for x in self.db.xpath('$.store.book[2:].author')]
        print rslt, self.all_authors[2:]
        assert rslt == self.all_authors[2:]

    def test_query_range_mn(self):
        rslt = [x.value for x in self.db.xpath('$.store.book[-2:].author')]
        print rslt, self.all_authors[-2:]
        assert rslt == self.all_authors[-2:]

    def test_query_range_nm(self):
        rslt = [x.value for x in self.db.xpath('$.store.book[:-1].author')]
        assert rslt == self.all_authors[:-1]
        rslt = [x.value for x in self.db.xpath('$.store.book[:-2].author')]
        assert rslt == self.all_authors[:-2]

    def test_query_range_mm(self):
        rslt = [x.value for x in self.db.xpath('$.store.book[-2:-1].author')]
        assert rslt == self.all_authors[-2:-1]
        rslt = [x.value for x in self.db.xpath('$.store.book[-9:-2].author')]
        print rslt, self.all_authors[-9:-2]
        assert rslt == self.all_authors[-9:-2]

    def test_query_range_union(self):
        rslt = [x.value for x in self.db.xpath('$.store.book[1, 2, -1].author')]
        assert rslt == self.all_authors[:2] + self.all_authors[-1]

    def test_query_slicing_step(self):
        rslt = [x.value for x in self.db.xpath('$.store.book[-4:-1:-1].author')]
        assert rslt == self.all_authors[-4:-1:-1]
        rslt = [x.value for x in self.db.xpath('$.store.book[-4:-1:-2].author')]
        assert rslt == self.all_authors[-4:-1:-2]

    def test_query_recursive_descent(self):
        rslt = [x.value for x in self.db.xpath('$.store..price')]
        assert rslt == self.all_prices
        rslt = [x.value for x in self.db.xpath('$..price')]
        assert rslt == self.all_prices

    def test_query_exists(self):
        rslt = [x.value for x in self.db.xpath('$.store.book[?(@.isbn)].author')]
        assert rslt == self.all_authors[-2:]
 

def test_large():
    db = JsonDB.create('large.db', root_type=DICT)
    for i in range(1000):
        li = db.feed({str(i):{'value':str(i)}})

    #db.dump('large.json')
    db.close()

    db = JsonDB.load('large.db')
    assert [x.value for x in db.xpath('$.15.value')][0] == str(15)

if __name__ == '__main__':
    pass
