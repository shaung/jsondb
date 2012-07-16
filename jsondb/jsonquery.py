# -*- coding: utf-8 -*-

import re
import pyPEG
from pyPEG import parseLine 
from pyPEG import Symbol, keyword, _and, _not

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
def not_expr():         return 0, not_op, in_expr
def in_op():            return re.compile(r'in|not[ ]+in', re.I)
def in_expr():          return cmp_expr, -1, (in_op, '(', expr_list, ')')
def cmp_op():           return re.compile(r"\=+|\!\=|\<\=|\<|\>\=|\>")
def cmp_expr():         return like_expr, -1, (cmp_op, cmp_expr)
def like_op():          return re.compile(r'like|not[ ]+like', re.I)
def like_expr():        return add_expr, -1, (like_op, like_expr)
def add_op():           return re.compile(r'\+|\-')
def add_expr():         return mul_expr, -1, (add_op, add_expr)
def mul_op():           return re.compile(r'\*|\\')
def mul_expr():         return atom, -1, (mul_op, mul_expr)
def atom():             return [func, number, literal, boolean, child, ('(', expr, ')')]
def expr():             return or_expr
def expr_list():        return expr, -1, (",", expr)
def func():             return func_name, "(", expr_list, ")"

def child():            return '@', child_node, -1, (child_node)
def child_node():       return tag

def axis():             return re.compile(r'\.+')
def tag():              return [(1, axis, '*'), (0, axis, '[', re.compile(r'".*?"'), ']'), (1, axis, re.compile(r'\w+'))]
def predicate():        return '(', expr, ')'
# TODO: expression support in slicing
def index():            return re.compile(r'[+-]?\d+')
def start():            return re.compile(r'[+-]?\d+')
def end():              return re.compile(r'[+-]?\d+')
def step():             return re.compile(r'[+-]?\d+')
def slicing():          return 0, start, ':', 0, end, 0, (':', step)
def union():            return ['*', slicing, index]
def filter():           return [('?', predicate), (union, -1, (',', union))]
def filter_list():      return ('[', filter, ']'), -1, ('[', filter, ']')
def node():             return tag, 0, filter_list

def jsonpath():         return "$", node, -1, (node,)


def parse(path):
    rslt = parseLine(path, pattern=jsonpath, resultSoFar = [], skipWS = True, skipComments = None, packrat = True)[0]
    return cst2json(rslt[0])


def unquote(string):
    result = string
    while len(result) > 1 and result[0] == result[-1] and result[0] in ('"', "'"):
        result = result[1:-1]
    return result

def cst2json(cst):
    # FIXME: Rewrite this later.
    if isinstance(cst, unicode) or isinstance(cst, str):
        return cst

    if type(cst) is Symbol:
        name = cst.__name__
        body = cst.what

        if name in ('node', 'child_node'):
            content = {}
            for e in body:
                if e.__name__ == 'tag':
                    d = {}
                    for child in e.what:
                        if isinstance(child, basestring):
                            d['name'] = unquote(child)
                        else:
                            d.update(cst2json(child))
                    content[e.__name__] = d
                elif e.__name__ == 'filter_list':
                    content[e.__name__] = []
                    for item in [cst2json(x) for x in e.what]:
                        if item:
                            content[e.__name__].append(item)
                else:
                    content[e.__name__] = cst2json(e.what)
            result = content
            result['type'] = name

        elif name == 'child':
            result = {name: cst2json(body)}

        elif name == 'slicing':
            result = {'type': name}
            for e in body:
                result[e.__name__] = e.what

        elif name == 'index':
            result = {'type': name}
            result['value'] = cst2json(body)

        elif name == 'predicate':
            result = {'type': name}
            result['expr'] = cst2json(body[0])['expr']

        elif name == 'filter':
            result = {
                'type'  : 'union',
                'value' : [],
            }
            if len(body) == 0:
                return {}
            for e in body:
                if e.__name__ == 'predicate':
                    return cst2json(e)
                if e.__name__ == 'union':
                    union = cst2json(e)
                    if union:
                        result['value'].append(union)
                    continue
                result['value'].append({
                    'type' : e.__name__,
                    'value': cst2json(e.what),
                })
            if not result['value']:
                return {}

        elif name == 'union':
            return {} if (len(body) == 0) else cst2json(body[0])

        elif name == 'func':
            result = {
                name : {
                    'name': cst2json(body[0])['func_name'],
                }
            }
            result[name].update(cst2json(body[1]))

        elif name == 'expr':
            return {name: cst2json(body[0])}

        elif name == 'not_expr':
            if len(body) == 2:
                return {
                    'type'  : name,
                    'op'    : cst2json(body[0].what).lower(),
                    'right' : cst2json(body[1]),
                }
            else:
                return cst2json(body[0])

        elif name.endswith('_expr'):
            if len(body) == 3:
                return {
                    'type'  : name,
                    'op'    : cst2json(body[1].what).lower(),
                    'left'  : cst2json(body[0]),
                    'right' : cst2json(body[2]),
                }
            else:
                return cst2json(body[0])

        elif name == 'atom':
            # func, number, literal, boolean, child, expr
            child = cst2json(body[0])
            value = child[body[0].__name__]
            if body[0].__name__ == 'literal':
                value = "'%s'" % unquote(value)
            result = {
                name: {
                    'type'  : body[0].__name__,
                    'value' : value,
                }
            }

        elif isinstance(body, basestring):
            result = {name: body}

        else:
            result = {name: [cst2json(e) for e in body]}
    else:
        result = [cst2json(e) for e in cst]

    return result

