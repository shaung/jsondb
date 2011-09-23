# -*- coding: utf-8 -*-

"""
    jsondb.tests
    ~~~~~~~~~~~~

    Tests for jsondb.
"""

from jsondb import *

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
    print 'test float'
    db = JsonDB.create('bar.db', root_type=FLOAT, value=1.2)
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
    h_dom = db.feed({'dom' : []})[0]
    db.feed({'love': 1}, h_dom)
    db.close()
    db = JsonDB.load('bar.db')
    print 'dumps', db.dumps()
    print db.xpath('$.files')
    print db.dumprows()

    print '-'*40
    print'test from file'
    if not os.path.exists('10.db'):
        db = JsonDB.from_file('10.db', '10.json')
        db.close()
    db = JsonDB.load('10.db')
    #print 'dumps', db.dumps()
    rslts = db.xpath('$.Domain')
    for _id, _name in rslts:
        print _id, _name
        print db.xpath('$.name', _id)
        print db.xpath('$.description', _id)
        print db.xpath('$.typedef.type_name', _id)
    rslts = db.xpath('$.Obj')
    for _id, _name in rslts:
        print _id, _name
        print db.xpath('$.name', _id)
        print db.xpath('$.description', _id)
    rslts = db.xpath('$.Rev')
    for _id, _name in rslts:
        print db.xpath('$.CreateDate', _id)
 
    if not os.path.exists('2.db'):
        db = JsonDB.from_file('2.db', '2.json')
        db.close()
    db = JsonDB.load('2.db')
    for i in range(10000):
        rslts = db.xpath('$.Obj')
        for _id, _name in rslts:
            print _id, _name
            print db.xpath('$.name', _id)
            print db.xpath('$.description', _id)
    db.close()
