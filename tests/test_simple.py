# -*- coding: utf-8 -*-

"""
    jsondb.tests
    ~~~~~~~~~~~~

    Tests for jsondb.
"""

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


def test_query():
    #db = JsonDB.from_file('bookstore.db', 'bookstore.json')
    db = JsonDB.load('bookstore.db')

    rslt = [x.value for x in db.xpath('$.store.book[0].title')]
    assert rslt == ['Sayings of the Century']


def test_large():
    db = JsonDB.create('large.db', root_type=DICT)
    for i in range(100000):
        li = db.feed({str(i):{'value':str(i)}})

    #db.dump('large.json')
    db.close()

    db = JsonDB.load('large.db')
    assert [x.value for x in db.xpath('$.15.value')][0] == str(15)

if __name__ == '__main__':
    test_list()
