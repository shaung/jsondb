# -*- coding: utf-8 -*-

class BackendBase(object):
    def __init__(self, *args, **kws):
        pass

    def get_path(self):
        raise NotImplementedError
 
    def commit(self):
        raise NotImplementedError
  
    def rollback(self):
        raise NotImplementedError
 
    def close(self):
        raise NotImplementedError
 
    def insert_root(self, *args, **kws):
        raise NotImplementedError

    def insert(self, *args, **kws):
        raise NotImplementedError

    def batch_insert(self, *args, **kws):
        raise NotImplementedError

    def update_link(self, *args, **kws):
        raise NotImplementedError

    def jsonpath(self, *args, **kws):
        raise NotImplementedError

    def dumprows(self, *args, **kws):
        raise NotImplementedError

    def set_value(self, *args, **kws):
        raise NotImplementedError

    def get_row(self, *args, **kws):
        raise NotImplementedError

    def iter_children(self, *args, **kws):
        raise NotImplementedError

