# -*- coding: utf-8 -*-
"""
@create: 2019-05-30 22:17:18.

@author: ppolxda

@desc:
"""
import datetime

SUB_OP_FUNC = ['$addToSet', '$each']
OP_FUNCS = ['$max', '$min', '$inc', '$currentDate',
            '$unset', '$set', '$setOnInsert', '$rename']


def op_rename(data, update):
    for oldkey, newkey in update.items():
        try:
            value = data.pop(oldkey)
        except KeyError:
            continue
        finally:
            data[newkey] = value
    return data


def op_set(data, update):
    data.update(update)
    return data


def op_unset(data, update):
    for oldkey in update.keys():
        try:
            data.pop(oldkey)
        except KeyError:
            pass
    return data


def op_max(data, update):
    for key, value in update.items():
        if key in data:
            data[key] = max(value, data[key])
        else:
            data[key] = value
    return data


def op_min(data, update):
    for key, value in update.items():
        if key in data:
            data[key] = min(value, data[key])
        else:
            data[key] = value
    return data


def op_inc(data, update):
    for key, value in update.items():
        if key in data:
            data[key] += 1
        else:
            data[key] = value
    return data


def op_datetime(data, update):
    for key, value in update.items():
        if isinstance(value, dict) and '$type' in value:
            if value['$type'] == 'timestamp':
                data[key] = str(datetime.datetime.now())
            elif value['$type'] == 'date':
                data[key] = str(datetime.date.today())
            else:
                raise TypeError('op_datetime $type invaild')
        else:
            data[key] = str(datetime.datetime.now())
    return data
