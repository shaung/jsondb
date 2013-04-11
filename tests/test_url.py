# coding: utf8

import os
from nose.tools import eq_

from jsondb.backends.url import URL


def test_sqlite():
    url = 'sqlite3:///tmp/json.db'
    obj = URL.parse(url)
    eq_(obj.driver, 'sqlite3')
    eq_(obj.username, None)
    eq_(obj.password, None)
    eq_(obj.host, None)
    eq_(obj.port, None)
    database = 'tmp/json.db' if os.name == 'nt' else '/tmp/json.db'
    eq_(obj.database, database)
    eq_(unicode(obj), unicode(url))
    eq_(str(obj), url)
