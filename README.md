jsondb
======

Off-memory json for large data that does not fit the memory.

![Build Status](https://secure.travis-ci.org/shaung/jsondb.png?branch=develop)

Usage
------

    import jsondb
    
    # Create from nothing
    db = jsondb.create({})

    # Add somthing
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


License
-------

Released under the BSD license.
