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


db = jsondb.load('aaa.db')
"""
#[?(@.name != "")]
for t in db.query('$.Table[:]'):
    print t.data()['name']
    for col in t.query('$.cols[:]').values():
        print col['name']
        print col['attr_description']
        print '-' * 8
"""

for t in db.query('$.Table'):
    print t
    print t.get_datatype()
    for x in t.query('$.*'):
        print x.get_datatype(), x['name']
