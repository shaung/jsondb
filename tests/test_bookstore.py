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

logger = logging.getLogger(__file__)


import sqlite3
logger.debug('sqlite3 version = %s' % sqlite3.sqlite_version)


class TestBookStore:
    all_titles = ['Sayings of the Century', 'Sword of Honour', 'Moby Dick', 'The Lord of the Rings']
    all_authors = ['Nigel Rees', 'Evelyn Waugh', 'Herman Melville', 'J. R. R. Tolkien']
    all_prices = [8.95, 12.99, 8.99, 22.99, 19.95]

    def setup(self):
        self.dbpath = 'bookstore.db'
        if os.path.exists(self.dbpath):
            os.remove(self.dbpath)

        fpath = os.path.join(os.path.dirname(__file__), 'bookstore.json')
        db = JsonDB.from_file(self.dbpath, fpath)
        db.close()

        self.db = JsonDB.load(self.dbpath)
        #self.db.dumprows()

    def teardown(self):
        self.db.close()

    def test_condition_eq(self):
        path = '$.store.book[?(@.author="Evelyn Waugh")].title'
        self.eq(path, ['Sword of Honour'])

    def test_condition_eq_2(self):
        path = '$.store.book[?(@.author == "Evelyn Waugh")].title'
        self.eq(path, ['Sword of Honour'])

    def test_condition_ne(self):
        path = '$.store.book[?(@.author != "Evelyn Waugh")].title'
        self.eq(path, ['Sayings of the Century', 'Moby Dick', 'The Lord of the Rings'])

    def test_condition_not_eq(self):
        path = '$.store.book[?(not (@.author == "Evelyn Waugh"))].title'
        self.eq(path, ['Sayings of the Century', 'Moby Dick', 'The Lord of the Rings'])
 
    def test_condition_not_or(self):
        path = '$.store.book[?(not (@.author != "Evelyn Waugh") or @.price = 12.99)].title'
        self.eq(path, self.all_titles)
 
    def test_condition_like_exact(self):
        path = '$.store.book[?(@.author like "Evelyn Waugh")].title'
        self.eq(path, ['Sword of Honour'])
 
    def test_condition_like(self):
        path = '$.store.book[?(@.author like "Evelyn%")].title'
        self.eq(path, ['Sword of Honour'])
 
    def test_condition_not_like(self):
        path = '$.store.book[?(@.author not like "%i%")].title'
        self.eq(path, ['Sword of Honour'])
 
    def test_condition_in(self):
        path = '$.store.book[?(@.author in ("Herman Melville", "J. R. R. Tolkien"))].title'
        self.eq(path, ['Moby Dick', 'The Lord of the Rings'])
 
    def test_condition_not_in(self):
        path = '$.store.book[?(@.author not in ("Herman Melville", "J. R. R. Tolkien"))].title'
        self.eq(path, ['Sayings of the Century', 'Sword of Honour'])
 
    def test_condition_not_in_2(self):
        path = '$.store.book[?(not (@.author in ("Herman Melville", "J. R. R. Tolkien")))].title'
        self.eq(path, ['Sayings of the Century', 'Sword of Honour'])

    def test_condition_price_eq(self):
        path = '$.store.book[?(@.price=12.99)].title'
        self.eq(path, ['Sword of Honour'])

    def test_condition_price_eq_2(self):
        path = '$.store.book[?(@.price == 12.99)].title'
        self.eq(path, ['Sword of Honour'])

    def test_condition_price_ne(self):
        path = '$.store.book[?(@.price != 12.99)].title'
        self.eq(path, ['Sayings of the Century', 'Moby Dick', 'The Lord of the Rings'])

    def test_condition_price_gt(self):
        path = '$.store.book[?(@.price > 12.99)].title'
        self.eq(path, ['The Lord of the Rings'])

    def test_condition_price_lt(self):
        path = '$.store.book[?(@.price < 12.99)].title'
        self.eq(path, ['Sayings of the Century', 'Moby Dick'])

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
        self.db = JsonDB.create(data)


if __name__ == '__main__':
    pass
