# -*- coding: utf-8 -*-

"""
    jsondb.tests
    ~~~~~~~~~~~~

    Tests for jsondb.
"""

import os, json
import jsondb
from nose.tools import eq_

import logging
logging.basicConfig(level='DEBUG')

logger = logging.getLogger(__file__)


import sqlite3
logger.debug('sqlite3 version = %s' % sqlite3.sqlite_version)


class TestBase:
    def eq_dumps(self, root_type, data):
        db = JsonDB.create(data)
        dbpath = db.get_path()
        db.close()
        db = JsonDB.load(dbpath)
        eq_(db.dumps(), json.dumps(data))
        eq_(db.data(), data)

class TestFloat(TestBase):
    def setup(self):
        self.obj = 999.99
        self.db = jsondb.create(self.obj)

    def teardown(self):
        self.db.close()

    def test_add(self):
        eq_(self.db + 1, self.obj + 1)
 
    def test_radd(self):
        eq_(1 + self.db, 1 + self.obj)
 

class TestAccess(TestBase):
    def setup(self):
        self.obj = {
            "glossary": {
                "title": "example glossary",
                "persons" : [{
                    "name": "foo",
                    "tag": ["a", "B", 1]
                }, {
                    "name": "bar",
                    "tag": ["b", "B", 2]
                }],
                "GlossDiv": {
                    "title": "S",
                    "GlossList": {
                        "GlossEntry": {
                            "ID": "SGML",
                            "SortAs": "SGML",
                            "GlossTerm": "Standard Generalized Markup Language",
                            "Acronym": "SGML",
                            "Abbrev": "ISO 8879:1986",
                            "GlossDef": {
                                "para": "A meta-markup language, used to create markup languages such as DocBook.",
                                "GlossSeeAlso": ["GML", "XML"]
                            },
                            "GlossSee": "markup"
                        }
                    }
                }
            }
        }
        self.db = jsondb.create(self.obj)
        self.db.dumprows()

    def teardown(self):
        self.db.close()


class TestDict(TestAccess):
    def test_dict_query(self):
        eq_(self.db['glossary'].data(), self.obj['glossary'])

    def test_dict_len(self):
        eq_(len(self.db['glossary']), len(self.obj['glossary']))
        eq_(self.db['glossary']._get_value(), len(self.obj['glossary']))

    def test_dict_clear(self):
        self.db['glossary'].clear()
        eq_(self.db['glossary'].data(), {})

    def test_dict_delete(self):
        g = self.db['glossary']
        del g['title']
        data = self.obj['glossary']
        del data['title']
        eq_(g.data(), data)

    def test_dict_set(self):
        self.db['glossary']['count'] = 1
        eq_(self.db['glossary']['count'].data(), 1)

        self.db['glossary'].update({'count': 2})
        eq_(self.db['glossary']['count'].data(), 2)

        self.db['glossary'].update({'somestr': 'foo'})
        eq_(self.db['glossary']['somestr'].data(), 'foo')

        self.db['glossary'].update({'somestr': [1, 2]})
        eq_(self.db['glossary']['somestr'].data(), [1, 2])

    def test_dict_items(self):
        for k, v in self.db['glossary'].items():
            eq_(v, self.obj['glossary'][k])

    def test_dict_iteritems(self):
        for k, v in self.db['glossary'].iteritems():
            eq_(v, self.obj['glossary'][k])


class TestList(TestAccess):
    def test_list(self):
        eq_(self.db['glossary']['persons'].data(), self.obj['glossary']['persons'])
        eq_(len(self.db['glossary']['persons']), 2)

        new_person = {'name': '3rd', 'tag': ['c', 'C', 3]}
        self.db['glossary']['persons'].append(new_person)
        eq_(self.db['glossary']['persons'][2].data(), new_person)
        eq_(self.db['glossary']['persons'][-1].data(), new_person)
        try:
            self.db['glossary']['persons'][3].data()
            self.db['glossary']['persons'][-4].data()
        except IndexError:
            pass
        else:
            raise

        # imul
        self.db['glossary']['persons'][2]['tag'] *= 3
        eq_(self.db['glossary']['persons'][2]['tag'].data(), new_person['tag'] * 3)

        self.db['glossary']['persons'][2]['tag'] = new_person['tag']

        # mul
        tag = self.db['glossary']['persons'][2]['tag']
        eq_(tag * 3, new_person['tag'] * 3)
        eq_(3 * tag, 3 * new_person['tag'])

        # iadd
        tag = self.db['glossary']['persons'][2]['tag']
        tag += ['tail']
        eq_(tag.data(), new_person['tag'] + ['tail'])
        eq_(self.db['glossary']['persons'][2]['tag'].data(), new_person['tag'] + ['tail'])

        # iadd
        self.db['glossary']['persons'][2]['tag'] += ['other']
        eq_(self.db['glossary']['persons'][2]['tag'].data(), new_person['tag'] + ['tail', 'other'])

        # dict assign
        self.db['glossary']['persons'][2]['tag'] = new_person['tag'] + ['tail']
        eq_(self.db['glossary']['persons'][2]['tag'].data(), new_person['tag'] + ['tail'])

        # list assign
        self.db['glossary']['persons'][0] = new_person
        eq_(self.db['glossary']['persons'][0].data(), new_person)

    def test_max(self):
        self.db['glossary']['numbers'] = range(10)
        eq_(max(self.db['glossary']['numbers']).data(), 9)
        eq_(min(self.db['glossary']['numbers']).data(), 0)


class TestInteger(TestAccess):
    def setup(self):
        TestAccess.setup(self)
        self.num = self.db['glossary']['persons'][0]['tag'][-1]
        self.vl = self.obj['glossary']['persons'][0]['tag'][-1]

    def test_int(self):
        num = self.db['glossary']['persons'][0]['tag'][-1]
        vl = self.obj['glossary']['persons'][0]['tag'][-1]
        eq_(num, vl)
        i = 2

        eq_(~num, ~vl)
        eq_(+num, +vl)
        eq_(-num, -vl)
        eq_(abs(num), abs(vl))

        eq_(num + i, vl + i)
        eq_(num - i, vl - i)
        eq_(num * i, vl * i)
        eq_(num / i, vl / i)
        eq_(num // i, vl // i)
        eq_(num % i, vl % i)
        eq_(num ** i, vl ** i)
        eq_(num & i, vl & i)
        eq_(num | i, vl | i)
        eq_(num ^ i, vl ^ i)
        eq_(num << i, vl << i)
        eq_(num >> i, vl >> i)

        eq_(i + num, i + vl)
        eq_(i - num, i - vl)
        eq_(i * num, i * vl)
        eq_(i / num, i / vl)
        eq_(i // num, i // vl)
        eq_(i % num, i % vl)
        eq_(i ** num, i ** vl)
        eq_(i & num, i & vl)
        eq_(i | num, i | vl)
        eq_(i ^ num, i ^ vl)
        eq_(i << num, i << vl)
        eq_(i >> num, i >> vl)

    def test_int_iadd(self):
        i = 2
        self.num += i
        self.vl += i
        eq_(self.num, self.vl)

    def test_int_isub(self):
        i = 2
        self.num -= i
        self.vl -= i
        eq_(self.num, self.vl)

    def test_int_imul(self):
        i = 2
        self.num *= i
        self.vl *= i
        eq_(self.num, self.vl)

    def test_int_ifloordiv(self):
        i = 2
        self.num //= i
        self.vl //= i
        eq_(self.num, self.vl)

    def test_int_idiv(self):
        i = 2
        self.num /= i
        self.vl /= i
        eq_(self.num, self.vl)

    def test_int_imod(self):
        i = 2
        self.num %= i
        self.vl %= i
        eq_(self.num, self.vl)

    def test_int_ipow(self):
        i = 2
        self.num **= i
        self.vl **= i
        eq_(self.num, self.vl)

    def test_int_iand(self):
        i = 2
        self.num &= i
        self.vl &= i
        eq_(self.num, self.vl)

    def test_int_ior(self):
        i = 2
        self.num |= i
        self.vl |= i
        eq_(self.num, self.vl)

    def test_int_ixor(self):
        i = 2
        self.num ^= i
        self.vl ^= i
        eq_(self.num, self.vl)

    def test_int_ilshift(self):
        i = 2
        self.num <<= i
        self.vl <<= i
        eq_(self.num, self.vl)

    def test_int_irshift(self):
        i = 2
        self.num >>= i
        self.vl >>= i
        eq_(self.num, self.vl)


class TestString(TestAccess):
    def setup(self):
        TestAccess.setup(self)
        self.s = self.db['glossary']['title']
        self.v = self.obj['glossary']['title']

    def test_op(self):
        s = self.s
        v = self.v
        eq_(s, v)
        eq_(s.data(), v)
        other = 'test'
        eq_(s + other, v + other)
        eq_(other + s, other + v)
        eq_(s * 2, v * 2)
        eq_(2 * s, 2 * v)
        eq_(s[2], v[2])
        eq_(s[1:-1:-1], v[1:-1:-1])
        eq_(len(s), len(v))
        eq_(max(s), max(v))
        eq_(min(s), min(v))
        eq_('l' in s, 'l' in v)
        eq_('l' not in s, 'l' not in v)

        eq_(s.index('l'), v.index('l'))
        eq_(s.count('e'), v.count('e'))
        eq_(s.lower(), v.lower())

        self.db['glossary']['fmt'] = '-%s-'
        eq_(self.db['glossary']['fmt'] % 'hello', '-hello-')

        self.db['glossary']['title'] = 'x'
        eq_(self.db['glossary']['title'], 'x')

    def test_iadd(self):
        self.s += 'foo'
        self.v += 'foo'
        eq_(self.s, self.v)

    def test_imul(self):
        self.s *= 3
        self.v *= 3
        eq_(self.s, self.v)


def test_sample():
    import jsondb
    
    # Create from nothing
    db = jsondb.create({})

    # Insert
    db['name'] = 'foo'

    db['friends'] = []
    for i in range(3):
        db['friends'].append({
            'id' : i,
            'name': chr(97 + i),
        })

    # It works like an ordinary dict
    assert db['name'] == 'foo'
    assert db['friends'][0]['id'] == 0
    assert len(db['friends']) == 3
    assert db.get('nonexists', 'notfound') == 'notfound'

    # Get the *real* data
    assert db.data() == {
        'name' : 'foo',
        'friends': [
            {'id' : 0, 'name' : 'a'},
            {'id' : 1, 'name' : 'b'},
            {'id' : 2, 'name' : 'c'},
        ]
    }
   
    # Query using jsonquery
    db.query('$.name').getone() == 'foo'

    # Iterating the query result
    #   => "a b c"
    for x in db.query('$.friends.name'):
        print x.data(),

    # Conditonal query
    eq_(db.query('$.friends[?(@.id = 1)].name').getone(), 'b')


    friends = db['friends']
    eq_(friends.query('$.name').values(), ['a', 'b', 'c'])
