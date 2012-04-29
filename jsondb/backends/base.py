# -*- coding: utf-8 -*-

class BackendBase(object):
    def __init__(self, *args, **kws):
        pass

    def get_cursor(self):
        pass

    def commit(self):
        pass

    def insert_root(self, root_type, value):
        pass
