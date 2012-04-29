# -*- coding: utf-8 -*-

"""
    jsondb.tests
    ~~~~~~~~~~~~

    Tests for jsondb.
"""

import sys
sys.path.append('/usr/local/lib/python2.6/dist-packages')
from jsondb import *

if __name__ == '__main__':

    print '-'*40
    print 'test string'
    db = JsonDB.create('foo.db', root_type=STR, value='hello world!')
    print db.dumprows()
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
    print '1'
    db.feed('world!')
    print '2'
    db.feed([1, 2])
    print 'dumps', db.dumps()
    db.close()
    db = JsonDB.load('bar.db')
    #print db.dumps()
    assert db.dumps() == ['hello', 'world!', [1.0, 2.0]]



    if not os.path.exists('2.db'):
        db = JsonDB.from_file('2.db', '2.json')
        #db.build_index()
        db.close()
    db = JsonDB.load('2.db')
    rslts = db.xpath('$.Project.Obj')
    for _id, _name, link in rslts:
        print _id, _name, link
        print db.xpath('$.name', _id)
        print db.xpath('$.description', _id)
    print db.xpath('$.Project.Obj[0].name')
    print db.xpath('$.Project.Obj[0].description')
    print db.xpath('$.Project.Obj[-1].name')
    print db.xpath('$.Project.Obj[-1].description')

    db.close()

    print '-'*40
    print'test dict'
    db = JsonDB.create('bar.db', root_type=DICT)
    _id = db.feed({'name': []})[0]
    db.feed({'files': ['xxx.py', 345, None, True, 'wtf', {'foo' : ['f1', 'f2']}]}, _id)
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
    print 'dumps', db.dumps()
    print db.xpath('$.files[0]')
    print db.xpath('$.files[1]')
    print db.xpath('$.files[2]')
    print db.xpath('$.files[3]')
    print db.xpath('$.files[4]')
    print db.xpath('$.files[5]')
    print db.xpath('$.files[-1]')
    print db.xpath('$.files[-2]')
    print db.xpath('$.files[-234]')
    print db.dumprows()

    print db.xpath('$.name[?(@.crazy in ("2", "4"))].bloon')
    print db.xpath('$.name[-1:].bloon')

    print '-'*40
    print'test from file'
    if not os.path.exists('10.db'):
        db = JsonDB.from_file('10.db', '10.json')
        #db.build_index()
        db.close()
    db = JsonDB.load('10.db')
    #print 'dumps', db.dumps()
    rslts = db.xpath('$.Domain')
    for _id, _name, link in rslts:
        print _id, _name
        print db.xpath('$.name', _id)
        print db.xpath('$.description', _id)
        print db.xpath('$.typedef.type_name', _id)
    rslts = db.xpath('$.Obj')
    for _id, _name, link in rslts:
        db.update_link(_id, 'i am a link for %s!' % _id)
    db.commit()

    rslts = db.xpath('$.Obj')
    for _id, _name, link in rslts:
        print _id, _name, link
        print db.xpath('$.name', _id)
        print db.xpath('$.description', _id)

    for i in range(1000):
        db.xpath('$.Obj[?(@.type in ("WebPanel", "Transaction"))].name')
        db.xpath('$.Obj[?(@.type = "WebPanel")].name')
     
