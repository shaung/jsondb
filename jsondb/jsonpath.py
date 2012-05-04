# -*- coding: utf-8 -*-

import re

from constants import KEY


def _break_path(path):
    # here we ignore the '..' expr for now
    path = path[2:]
    conditions = []
    def func(m):
        rslt = '__%s__' % len(conditions)
        conditions.append(m.group(0))
        return rslt

    normed = re.sub(r'\[(.*?)\]', func, path)
    groups = normed.split('.')

    def recover(m):
        return conditions[int(m.group(1))]
    return [re.sub(r'__([0-9]*)__', recover, g) for g in groups]


def _get_cond(expr):
    cond = ''
    extra = ''
    order = 'asc'
    reverse = False

    if expr[-1] != ']':
        name = expr
        return name, cond, extra, order, reverse

    name, cond = expr[:-1].split('[')
    if cond.startswith('?'):
        extra = ''
        # for sub query
        cond = cond[2:-1]

        def f(m):
            item = m.group(1)
            condition = m.group(2)
            # FIXME: quick dirty adhoc solution
            condition = condition.replace('True', '1').replace('False', '0').replace('"', "'")
            return """ exists (select tv.id from jsondata tv
                        where tv.parent in (select tk.id from jsondata tk 
                        where tk.value = '%s' and tk.parent = t.id and tk.type = %s ) and tv.value %s )""" % (item, KEY, condition)

        # break ands and ors
        conds = []
        for _and in cond.split(' and '):
            conds.append(' or '.join(re.sub(r'@\.(\w+)(.*)', f, _or) for _or in _and.split(' or ')))
        cond = ' and %s ' % (' and '.join(conds))
    else:
        # for lists
        while cond.startswith('(') and cond.endswith(')'):
            cond = cond[1:-1]
        if cond and cond != '*':
            if ':' not in cond:
                nth = int(cond)
                if nth < 0:
                    order = 'desc'
                    nth *= -1
                    nth -= 1
                extra = 'limit 1 offset %s' % nth
            else:
                extra, order, reverse = parse_range(cond)
        cond = ''

    return name, cond, extra, order, reverse


def parse_range(cond):
    extra = ''
    order = 'asc'
    reverse = False
 
    start, end = [x.strip() for x in cond.split(':')]
    if start == '0':
        start = None
    if end == '0':
        return extra, order, reverse

    if not start and not end or start == end:
        return extra, order, reverse

    if start and end:
        start, end = int(start), int(end)
        # TODO: null ranges
        # [1:0]
        # [-1:0]
        # [-1:-2]

        # valid ranges
        # [1:2]
        # [-2:-1]
        # [1:-1]
        limit = -1
        if start >= 0 and end > 0:
            limit = end - start + 1
        elif start < 0 and end < 0:
            limit = end - start
            start = end * -1
            order = 'desc'
            reverse = True
        elif start > 0 and end < 0:
            # TODO: The tricky part. Typically you cannot do such things with sql...
            pass

        extra = 'limit %s offset %s' % (limit, start)

    elif start:
        start = int(start)
        if start > 0:
            # [1:]
            extra = 'limit %s offset %s' % (start, start)
        elif start < 0:
            # [-1:]
            start *= -1
            order = 'desc'
            extra = 'limit %s offset %s' % (start, 0)
            reverse = True
        else:
            # [0:]
            pass

    elif end:
        end = int(end)
        if end >= 0:
            # [:1]
            extra = 'limit %s' % (end + 1)
        else:
            # TODO: the order can not be done directly.
            # [:-1]
            order = 'desc'
            extra = 'limit -1 offset %s' % (end * -1)
            reverse = True

    return extra, order, reverse
