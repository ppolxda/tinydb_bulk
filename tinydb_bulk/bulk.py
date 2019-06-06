# -*- coding: utf-8 -*-
"""
@create: 2019-05-30 22:26:12.

@author: ppolxda

@desc:
"""
import six
import hashlib
import itertools
from tinydb.database import Table
from .indexs import IndexsMrg
from .errors import Error, IndexExpiredError, InputError
from . import mongo_op as mop

reduce = six.moves.reduce


class TableBulk(object):
    """TableBulk."""

    def __init__(self, indexs, table: Table):
        # self.db = db
        self.datas = table
        self.bulk = self.datas.bulk()
        self.indexs = IndexsMrg(indexs)
        self.reset()

    def find(self, _filter):
        if not isinstance(_filter, dict):
            raise InputError('_filter invaild')

        ids = self.indexs.find_hash(_filter)
        datas = [
            self.datas.get(doc_id=_id._id)
            for _id in ids
        ]
        return datas

    def find_one(self, _filter):
        if not isinstance(_filter, dict):
            raise InputError('_filter invaild')

        ids = self.indexs.find_hash(_filter)
        if not ids:
            return None

        data = self.datas.get(doc_id=next(iter(ids))._id)
        return data

    def __find_one_in_bulk(self, _filter, index=-1):
        # find hash
        ids = self.indexs.find_hash(_filter)
        if not ids:
            return None

        _id = next(iter(ids))
        data = self.bulk.get(doc_id=_id._id)
        return _id._id, data

    def upsert_one(self, _filter, update, index=-1):
        if not isinstance(_filter, dict):
            raise InputError('_filter invaild')

        if not isinstance(update, dict):
            raise InputError('_filter invaild')

        # is indexs not same
        if self.indexs.is_need_reindex():
            self.reset()
            raise IndexExpiredError('index expired')

        data = self.__find_one_in_bulk(_filter, index)
        if not data:
            _id = None
            data = {}
            data.update(_filter)
            if '$setOnInsert' in update:
                data.update(update['$setOnInsert'])
        else:
            _id = data[0]
            data = data[1]

        for op, parames in update.items():
            if not isinstance(parames, dict):
                raise InputError('optype parames invaild[{}][{}]'.format(
                    op, parames
                ))

            if op == '$set':
                data = mop.op_set(data, parames)
            elif op == '$unset':
                data = mop.op_unset(data, parames)
            elif op == '$rename':
                data = mop.op_rename(data, parames)
            elif op == '$max':
                data = mop.op_max(data, parames)
            elif op == '$min':
                data = mop.op_min(data, parames)
            elif op == '$inc':
                data = mop.op_inc(data, parames)
            elif op == '$addToSet':
                data = mop.op_addtoset(data, parames)
            elif op == '$currentDate':
                data = mop.op_datetime(data, parames)
            elif op == '$setOnInsert':
                pass
            else:
                raise InputError('optype invaild[{}]'.format(op))

        if _id:
            doc_id = _id
            try:
                self.indexs.upsert_one(doc_id, data)
            except Error:
                # rollback, only use unique
                self.reset()
                raise

            self.bulk.update(data, doc_ids=[doc_id])
        else:
            doc_id = self.bulk.insert(data)

            try:
                self.indexs.upsert_one(doc_id, data)
            except Error:
                # rollback, only use unique
                self.reset()
                raise

    def flush(self):
        try:
            self.bulk.flush()
        except Exception:
            raise
        finally:
            self.reset()

    def reset(self):
        # TODO - big file too slow
        self.bulk = self.datas.bulk()
        self.indexs.reindex(self.bulk.all())
