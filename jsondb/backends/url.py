# coding: utf8

from urlparse import urlparse, urlunsplit
from jsondb.util import IS_WINDOWS


class Error(Exception):
    pass


class URL(object):
    def __init__(self, driver, username=None, password=None,
                 host=None, port=None, database=None):
        self.driver = driver
        self.username = username
        self.password = password
        self.host = host
        self.port = int(port) if port else None
        self.database = database

    def __unicode__(self):
        auth = '%s%s%s' % (self.username or '', ':' if (self.username or self.password) else '', self.password or '')
        host = '%s%s%s' % (self.host or '', ':' if (self.host or self.port) else '', self.port or '')
        netloc = '%s%s%s' % (auth, '@' if auth else '', host)
        database = ('/' if (self.driver == 'sqlite3' and IS_WINDOWS and not self.database.startswith('/')) else '') + self.database
        database = '%s%s' % ('' if netloc else '//', database)
        parts = (self.driver, netloc, database, '', '')
        return urlunsplit(parts)

    def __str__(self):
        return unicode(self).encode('utf-8')

    @classmethod
    def parse(cls, url):
        if IS_WINDOWS:
            url = url.replace('\\', '/')
        if isinstance(url, basestring):
            url = urlparse(url)
        database = url.path
        if IS_WINDOWS and database.startswith('/'):
            database = database[1:]
        self = cls(driver=url.scheme, username=url.username, password=url.password,
                   host=url.hostname, port=url.port, database=database)
        return self
