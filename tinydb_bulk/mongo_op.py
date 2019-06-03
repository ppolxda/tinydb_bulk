# -*- coding: utf-8 -*-
"""
@create: 2019-05-30 22:17:18.

@author: ppolxda

@desc:
"""
import datetime
from .errors import InputError

OP_EACH = '$each'
OP_EACH_SITE = len(OP_EACH)
OP_EACH_INDEX = -1 * (OP_EACH_SITE + 1)
SUB_OP_FUNC = ['$addToSet', '$each']
OP_FUNCS = ['$max', '$min', '$inc', '$currentDate',
            '$unset', '$set', '$setOnInsert', '$rename']
CONV_IGNORE_OP = ['$each']


def op_rename(data, update):
    update = conv_doc2root(update)

    for oldkey, newkey in update.items():
        try:
            value = data.pop(oldkey)
        except KeyError:
            continue
        finally:
            data[newkey] = value
    return data


def op_set(data, update):
    update = conv_doc2root(update)
    data.update(update)
    return data


def op_unset(data, update):
    update = conv_doc2root(update)
    for oldkey in update.keys():
        try:
            data.pop(oldkey)
        except KeyError:
            pass
    return data


def op_max(data, update):
    update = conv_doc2root(update)
    for key, value in update.items():
        if key in data:
            data[key] = max(value, data[key])
        else:
            data[key] = value
    return data


def op_min(data, update):
    update = conv_doc2root(update)
    for key, value in update.items():
        if key in data:
            data[key] = min(value, data[key])
        else:
            data[key] = value
    return data


def op_inc(data, update):
    update = conv_doc2root(update)
    for key, value in update.items():
        if key in data:
            data[key] += value
        else:
            data[key] = value
    return data


def op_addtoset(data, update):
    update = conv_doc2root(update)

    for key, val in update.items():
        each = False
        if len(key) > OP_EACH_SITE and '$each' == key[OP_EACH_INDEX + 1:]:
            key = key[:OP_EACH_INDEX]
            each = True

        if each:
            if not isinstance(val, list):
                raise InputError('$addToSet invaild')

            if key not in data:
                data[key] = []

            for i in val:
                if i not in data[key]:
                    data[key].append(i)
        else:
            if key not in data:
                data[key] = []

            if val not in data[key]:
                data[key].append(val)
    return data


def op_datetime(data, update):
    update = conv_doc2root(update)

    for key, value in update.items():
        if isinstance(value, dict) and '$type' in value:
            if value['$type'] == 'timestamp':
                data[key] = str(datetime.datetime.now())
            elif value['$type'] == 'date':
                data[key] = str(datetime.date.today())
            else:
                raise InputError('op_datetime $type invaild')
        else:
            data[key] = str(datetime.datetime.now())
    return data


def __loop_doc2root(doc, prefix=None):
    for key, val in doc.items():
        if key.startswith('$') and key not in CONV_IGNORE_OP:
            raise InputError('loop_doc2root not support op conv')

        if prefix:
            _prefix = '.'.join([prefix, key])
        else:
            _prefix = key

        if isinstance(val, dict):
            yield from __loop_doc2root(val, _prefix)
        else:
            yield _prefix, val


def conv_doc2root(doc):
    if not isinstance(doc, dict):
        raise InputError('loop_root2doc doc invaild')
    return {key: val for key, val in __loop_doc2root(doc)}


def __create_root2doc(doc, val, keys):
    key = keys.pop(0)
    if key not in doc:
        doc[key] = {}

    if keys:
        __create_root2doc(doc[key], val, keys)
    else:
        doc[key] = val


def conv_root2doc(doc):
    if not isinstance(doc, dict):
        raise InputError('loop_doc2root doc invaild')

    result = {}
    for key, val in doc.items():
        __create_root2doc(result, val, key.split('.'))
    return result
