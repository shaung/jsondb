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


