from jsondb.jsonquery import parse

print parse('$.store.book[?(@.author like "Evelyn Waugh")].title')
print parse('$.store.book[?(@.author = "Evelyn Waugh")].title')
print parse('$.store.book[?(@.author not like "%n%")].title')
print parse('$.store.book[?(@.author in ("%n%"))].title')
