# -*- coding: utf-8 -*-
"""
@create: 2019-05-30 22:17:18.

@author: ppolxda

@desc:
"""
import six
import datetime
from functools import partial
from .errors import InputError

SUB_OP_FUNC = ['$addToSet', '$each']
OP_FUNCS = ['$max', '$min', '$inc', '$currentDate',
            '$unset', '$set', '$setOnInsert', '$rename']
CONV_IGNORE_OP = ['$each']


def _replace_dict(func, doc, val, keys):
    key = keys.pop(0)
    if keys:
        if key not in doc:
            doc[key] = {}

        if not isinstance(doc[key], dict):
            raise InputError("Cannot create field '{}' in element".format(key))

        _replace_dict(func, doc[key], val, keys)
    else:
        func(doc, key, val)


def _op_rename(root, doc, oldkey, newkey):
    try:
        value = doc.pop(oldkey)
    except KeyError:
        raise InputError("Cannot found field '{}' in doc".format(oldkey))
    else:
        op_set(root, {newkey: value})


def op_rename(data, update):
    __op_rename = partial(_op_rename, data)
    for oldkey, newkey in update.items():
        _replace_dict(__op_rename, data, newkey, oldkey.split('.'))
    return data


def _op_set(doc, key, val):
    doc[key] = val


def op_set(data, update):
    for key, val in update.items():
        _replace_dict(_op_set, data, val, key.split('.'))
    return data


def _op_unset(doc, key, val):
    try:
        doc.pop(key)
    except KeyError:
        raise InputError("Cannot found field '{}' in doc".format(key))


def op_unset(data, update):
    for key in update.keys():
        _replace_dict(_op_unset, data, '', key.split('.'))
    return data


def _op_max(doc, key, val):
    if not isinstance(val,  (six.integer_types, float,
                             six.string_types, six.binary_type)):
        raise InputError(
            'Cannot apply $max to a value of invaild type.'
            'has the field {} of invaild type string'.format(key)
        )

    if key in doc:
        if not isinstance(doc[key],  (six.integer_types, float,
                                      six.string_types, six.binary_type)):
            raise InputError(
                'Cannot apply $max to a value of invaild type.'
                'has the field {} of invaild type string'.format(key)
            )
        doc[key] = max(val, doc[key])
    else:
        doc[key] = val


def op_max(data, update):
    for key, val in update.items():
        _replace_dict(_op_max, data, val, key.split('.'))
    return data


def _op_min(doc, key, val):
    if not isinstance(val,  (six.integer_types, float,
                             six.string_types, six.binary_type)):
        raise InputError(
            'Cannot apply $min to a value of invaild type.'
            'has the field {} of invaild type string'.format(key)
        )

    if key in doc:
        if not isinstance(doc[key],  (six.integer_types, float,
                                      six.string_types, six.binary_type)):
            raise InputError(
                'Cannot apply $min to a value of invaild type.'
                'has the field {} of invaild type string'.format(key)
            )
        doc[key] = min(val, doc[key])
    else:
        doc[key] = val


def op_min(data, update):
    for key, val in update.items():
        _replace_dict(_op_min, data, val, key.split('.'))
    return data


def _op_inc(doc, key, val):
    if not isinstance(val,  (six.integer_types, float)):
        raise InputError(
            'Cannot apply $inc to a value of non-numeric type.'
            'has the field {} of non-numeric type string'.format(key)
        )

    if key in doc:
        if not isinstance(doc[key], (six.integer_types, float)):
            raise InputError(
                'Cannot apply $inc to a value of non-numeric type.'
                'has the field {} of non-numeric type string'.format(key)
            )

        doc[key] += val
    else:
        doc[key] = val


def op_inc(data, update):
    for key, val in update.items():
        _replace_dict(_op_inc, data, val, key.split('.'))
    return data


def _op_addtoset(data, key, val):
    each = False
    if isinstance(val, dict) and '$each' in val:
        each = True
        val = val['$each']

    if each:
        if not isinstance(val, (list, set)):
            raise InputError('$each invaild')

        if key not in data:
            data[key] = set()

        # list to slow use set
        if isinstance(data[key], list):
            data[key] = set(data[key])

        if not isinstance(data[key], (list, set)):
            raise InputError(
                'Cannot apply $addToSet to a value of non-array type.'
                'has the field {} of non-array type string'.format(key)
            )

        data[key] |= set(val)
    else:
        if key not in data:
            data[key] = set()

        # list to slow use set
        if isinstance(data[key], list):
            data[key] = set(data[key])

        if not isinstance(data[key], (list, set)):
            raise InputError(
                'Cannot apply $addToSet to a value of non-array type.'
                'has the field {} of non-array type string'.format(key)
            )

        if val not in data[key]:
            data[key].add(val)


def op_addtoset(data, update):
    for key, val in update.items():
        _replace_dict(_op_addtoset, data, val, key.split('.'))
    return data


def _op_datetime(data, key, value):
    if isinstance(value, dict) and '$type' in value:
        if value['$type'] == 'timestamp':
            data[key] = str(datetime.datetime.now())
        elif value['$type'] == 'date':
            data[key] = str(datetime.date.today())
        else:
            raise InputError('op_datetime $type invaild')
    else:
        data[key] = str(datetime.datetime.now())


def op_datetime(data, update):
    for key, val in update.items():
        _replace_dict(_op_datetime, data, val, key.split('.'))
    return data


def op_update(data, update):
    # when doc is null
    if not data and '$setOnInsert' in update:
        data.update(update['$setOnInsert'])

    for op, parames in update.items():
        if not isinstance(parames, dict):
            raise InputError('optype parames invaild[{}][{}]'.format(
                op, parames
            ))

        if op == '$set':
            data = op_set(data, parames)
        elif op == '$unset':
            data = op_unset(data, parames)
        elif op == '$rename':
            data = op_rename(data, parames)
        elif op == '$max':
            data = op_max(data, parames)
        elif op == '$min':
            data = op_min(data, parames)
        elif op == '$inc':
            data = op_inc(data, parames)
        elif op == '$addToSet':
            data = op_addtoset(data, parames)
        elif op == '$currentDate':
            data = op_datetime(data, parames)
        elif op == '$setOnInsert':
            pass
        else:
            raise InputError('optype invaild[{}]'.format(op))

# ----------------------------------------------
#        doc conv
# ----------------------------------------------


def __loop_doc2root(doc, prefix=None):
    for key, val in doc.items():
        if key.startswith('$') and key not in CONV_IGNORE_OP:
            raise InputError('conv_doc2root not support op conv')

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
        raise InputError('conv_doc2root doc invaild')
    return {key: val for key, val in __loop_doc2root(doc)}


def __create_root2doc(doc, val, keys):
    key = keys.pop(0)
    if keys:
        if key not in doc:
            doc[key] = {}
        __create_root2doc(doc[key], val, keys)
    else:
        doc[key] = val


def conv_root2doc(doc):
    if not isinstance(doc, dict):
        raise InputError('conv_root2doc doc invaild')

    result = {}
    for key, val in doc.items():
        __create_root2doc(result, val, key.split('.'))
    return result
