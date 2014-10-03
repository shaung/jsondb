def test():
    """
    >>> import jsondb
    
    >>> db = jsondb.create({})

    >>> db['name'] = 'foo'
    >>> db['friends'] = []
    >>> for i in range(3):
    ...     db['friends'].append({
    ...        'id' : i,
    ...        'name': chr(97 + i),
    ...     })
    ...

    >>> # It works like an ordinary dict
    >>> db['name'] == 'foo'
    True

    >>> db['friends'][0]['id'] == 0
    True

    >>> len(db['friends'])
    3

    >>> db.get('nonexists', 'notfound')
    'notfound'

    >>> # Get the data
    >>> assert db.data() == {
    ...     'name' : 'foo',
    ...     'friends': [
    ...         {'id' : 0, 'name' : 'a'},
    ...         {'id' : 1, 'name' : 'b'},
    ...         {'id' : 2, 'name' : 'c'},
    ...     ]
    ... }
    ...
   
    >>> # Query using jsonquery
    >>> db.query('$.name').getone() == 'foo'
    True

    >>> # Iterating the query result
    >>> #   => "a b c"
    >>> for x in db.query('$.friends.name'): print x.data(),
    ...
    a b c

    >>> # Conditonal query
    >>> #   => 'b'
    >>> print db.query('$.friends[?(@.id = 1)].name').getone().data()
    b
    >>> print db.query('$.friends[0:-1:-1].name').values()
    ['c', 'b']
    >>> print db.query('$.friends[?(@.id > 0)][::-1].name').values()
    ['c', 'b']
    """
    pass

if __name__ == '__main__':
    import doctest
    doctest.testmod()
