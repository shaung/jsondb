# -*- coding: utf-8 -*-

import types
from collections import namedtuple


(INT, FLOAT, STR, UNICODE, BOOL, NIL, LIST, DICT, KEY) = DATA_TYPES = range(9)
DATA_INITIAL = {
    INT     : 0,
    FLOAT   : 0.0,
    STR     : '',
    UNICODE : u'',
    BOOL    : False,
    NIL     : None,
    LIST    : [],
    DICT    : {},
    KEY     : '',
}

DATA_TYPE_NAME = {
    INT     : 'INT',
    FLOAT   : 'FLOAT',
    STR     : 'STR',
    UNICODE : 'UNICODE',
    BOOL    : 'BOOL',
    NIL     : 'NIL',
    LIST    : 'LIST',
    DICT    : 'DICT',
    KEY     : 'KEY',
}

TYPE_MAP = {
    types.IntType     : INT,
    types.LongType    : INT,
    types.FloatType   : FLOAT,
    types.StringType  : STR,
    types.UnicodeType : UNICODE,
    types.BooleanType : BOOL,
    types.NoneType    : NIL,
    types.ListType    : LIST,
    types.TupleType   : LIST,
    types.DictType    : DICT,
}


def get_datatype_class(_type):
    if _type == NIL:
        return None
    cls = DATA_INITIAL[_type].__class__
    return cls


def get_initial_data(_type):
    if _type == NIL:
        return None
    cls = DATA_INITIAL[_type].__class__
    return cls.__new__(cls)


class Result(namedtuple('Result', ('id', 'type', 'link'))):
    @classmethod
    def from_row(cls, row):
        self = cls(row['id'], row['type'], row['link'])
        return self
