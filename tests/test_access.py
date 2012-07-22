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

    def test_query(self):
        eq_(self.db['glossary'].data(), self.obj['glossary'])
        eq_(self.db['glossary']._get_value(), len(self.obj['glossary']))
        eq_(len(self.db['glossary']), len(self.obj['glossary']))
