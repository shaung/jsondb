jsondb
======


Off-memory json for large data that does not fit the memory.

Usage
------

    from jsondb import JsonDB
    
    data = {
        'name': 'foo',
    }
    
    db = JsonDB(value=data)
    
    assert db.query('$.name') == 'foo'

License
-------

Released under the BSD license.
