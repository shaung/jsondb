# jsondb

Off-memory json for large data that does not fit the memory.

![Build Status](https://secure.travis-ci.org/shaung/jsondb.png?branch=develop)


### Installation

To install, simply run

    python setup.py install


### Getting started

To create a db:

    import jsondb
    
    # Create from an empty dict (the default)
    db = jsondb.create({})

    # Or a list / tuple
    db = jsondb.create([])


to create from an existing JSON file:

    db = jsondb.from_file(json_file_path)
    # file-like objects are accepted as well
    db = jsondb.from_file(open(json_file_path, 'rb'))

Now add some data to the db and access them:

    db['name'] = 'foo'
    assert db['name'] == 'foo'

    db['items'] = []
    for i in range(3):
        db['items'].append({
            'id' : i,
            'name': chr(97 + i),
        })

    assert db['items'][0]['id'] == 0
    assert len(db['items']) == 3

    assert db.get('nonexists', 'notfound') == 'notfound'

It works like an ordinary dict / list,
but to get its value, use the `data()` method:

    # => 'foo'
    print db['name'].data() 

    # Get the data
    assert db.data() == {
        'name' : 'foo',
        'items': [
            {'id' : 0, 'name' : 'a'},
            {'id' : 1, 'name' : 'b'},
            {'id' : 2, 'name' : 'c'},
        ]
    }


### Querying

jsondb also supports complex queries, in a syntax based on JSONPath,
which is described here: http://goessner.net/articles/JsonPath/

`db.query` returns a `QueryResult` object which is iterable.

    results = db.query('$.items.name')

    # Iterating the query result
    #   => "a b c"
    for x in results:
        print x.data(),

To fetch all the value of the result,

    # => ['a', 'b', 'c']
    print rslt.values()

To fetch only one value,

    # fetch one result
    assert db.query('$.name').getone() == 'foo'
    # => 'foo'
    db.query('$.name').getone().data()

Example of more complex queries:

    # Conditonal query
    #   => 'b'
    print db.query('$.items[?(@.id = 1)].name').getone().data()

    # slicing
    # => ['a', 'b']
    print db.query('$.items[:-1].name').values()


### Persistence

All the data is stored on the disk.
In the current implementation, data is saved as a sqlite database,
but supports for other DBMS are on the table.

both of `create` and `from_file` accept a `url` parameter,
indicating where to store the data:

    db = jsondb.create({}, url='path/to/filename.db')
    # which is equal to 
    db = jsondb.create({}, url='sqlite3://path/to/filename.db')

When not specified, a temporary file will be created in `/tmp` by default.

To make sure all the changes made to db being saved to the file,

    db.save()

And when not needed anymore, remember to close it:

    db.close()

Or use context manager:

    with jsondb.create(url='path/to/filename.db') as db:
        # do all the work here

To load an existing jsondb file later,

    db = jsondb.load('path/to/filename.db')


### License

Released under the BSD license.
