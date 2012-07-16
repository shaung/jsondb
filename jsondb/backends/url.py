# coding: utf8

from urlparse import urlparse, urlunsplit

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
        parts = (self.driver, netloc, self.database, '', '')
        return urlunsplit(parts)

    @classmethod
    def parse(cls, url):
        if isinstance(url, basestring):
            url = urlparse(url)
        self = cls(driver=url.scheme, username=url.username, password=url.password,
            host=url.hostname, port=url.port, database=url.path)
        return self

