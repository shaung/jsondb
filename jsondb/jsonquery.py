# -*- coding: utf-8 -*-

import re
import pyPEG
from pyPEG import parseLine
from pyPEG import keyword, _and, _not

__all__ = ['parse']


def boolean():          return re.compile(r'True|False', re.I)
def number():           return re.compile(r'[+-]?\d*\.\d*|[+-]?\d+')
def literal():          return re.compile(r'".*?"')
def func_name():        return re.compile(r"\w+")
def or_op():            return re.compile(r'or', re.I)
def or_expr():          return and_expr, -1, (or_op, or_expr)
def and_op():           return re.compile(r'and', re.I)
def and_expr():         return not_expr, -1, (and_op, and_expr)
def not_op():           return re.compile(r'not', re.I)
def not_expr():         return [(not_op, in_expr), in_expr]
def in_op():            return re.compile(r'in', re.I)
def in_expr():          return cmp_expr, -1, (in_op, '(', exprs, ')')
def cmp_op():           return re.compile(r"\=\=|\!\=|\<\=|\<|\>\=|\>")
def cmp_expr():         return like_expr, -1, (cmp_op, cmp_expr)
def like_op():          return re.compile(r'like', re.I)
def like_expr():        return add_expr, -1, (like_op, literal)
def add_op():           return re.compile(r'\+|\-')
def add_expr():         return mul_expr, -1, (add_op, add_expr)
def mul_op():           return re.compile(r'\*|\\')
def mul_expr():         return atom, -1, (mul_op, mul_expr)
def atom():             return [func, number, literal, boolean, child, ('(', expr, ')')]
def expr():             return or_expr
def exprs():            return expr, -1, (",", expr)
def func():             return func_name, "(", exprs, ")"

def child():            return '@', child_nodes
def child_nodes():      return child_node, -1, (child_node)
def child_node():       return tag

def axis():             return re.compile(r'\.+')
def tag():              return [(1, axis, '*'), (0, axis, '[', re.compile(r'".*?"'), ']'), (1, axis, re.compile(r'\w+'))]
def predicate():        return '(', expr, ')'
# TODO: expression support in slicing
def index():            return re.compile(r'[+-]?\d+')
def start():            return re.compile(r'[+-]?\d+')
def end():              return re.compile(r'[+-]?\d+')
def step():             return re.compile(r'[+-]?\d+')
def ranging():          return 0, start, ':', 0, end, 0, (':', step)
def slicing():          return ['*', ranging, index]
def slicings():         return slicing, -1, (',', slicing)
def filter():           return [('?', predicate), slicings]
def filters():          return ('[', filter, ']'), -1, ('[', filter, ']')
def node():             return tag, -1, filters
def nodes():            return node, -1, (node,)

def jsonpath():         return "$", nodes


def parse(path):
    return parseLine(path, pattern=jsonpath, resultSoFar = [], skipWS = True, skipComments = None, packrat = True)[0]


if __name__ == '__main__':
    pass
