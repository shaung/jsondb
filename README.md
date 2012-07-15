jsondb
======

Off-memory json for large data that does not fit the memory.

![Build Status](https://secure.travis-ci.org/shaung/jsondb.png?branch=develop)

Usage
------

    from jsondb import JsonDB
    
    data = {
        'name': 'foo',
    }
    
    db = JsonDB(data)
    
    assert db.query('$.name') == 'foo'

License
-------

Released under the BSD license.
