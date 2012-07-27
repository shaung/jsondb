# -*- coding: utf-8 -*-

"""
jsondb.error
~~~~~~~~~~~~

"""


class Error(Exception):
    pass


class UnsupportedTypeError(Error):
    pass


class IllegalTypeError(Error):
    pass


class UnsupportedOperation(Error):
    pass
