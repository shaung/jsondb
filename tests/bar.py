import jsondb

data = {
    'name': 'foo',
}

db = jsondb.create(data)

print db['name'].data()

db['name'] = 1

print db['name'].data()
