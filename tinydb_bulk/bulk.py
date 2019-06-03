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
from .errors import Error
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
        ids = self.indexs.find_hash_key(_filter)
        datas = [self.datas.get(doc_id=_id._id) for _id in ids]
        return datas

    def find_one(self, _filter):
        ids = self.indexs.find_hash_key(_filter)
        if not ids:
            return None

        data = self.datas.get(doc_id=list(ids)[0]._id)
        return data

    def __find_one(self, _filter):
        ids = self.indexs.find_hash_key(_filter)
        if not ids:
            return None

        ids = list(ids)
        data = self.datas.get(doc_id=ids[0]._id)
        return ids[0]._id, data

    def upsert_one(self, _filter, update):
        data = self.__find_one(_filter)
        if not data:
            _id = None
            data = {}
        else:
            _id = data[0]
            data = data[1]

        if '$setOnInsert' in update and not data:
            data.update(update['$setOnInsert'])
            data.update(_filter)

        for op, parames in update.items():
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
            elif op == '$currentDate':
                data = mop.op_datetime(data, parames)
            elif op == '$setOnInsert':
                pass
            else:
                raise TypeError('optype invaild')

        if _id:
            doc_id = _id
            try:
                self.indexs.upsert_one(doc_id, data)
            except Error:
                # rollback, only use unique
                # TODO - too slow
                self.reset()
                raise

            self.bulk.update(data, doc_ids=[doc_id])
        else:
            doc_id = self.bulk.insert(data)

            try:
                self.indexs.upsert_one(doc_id, data)
            except Error:
                # rollback, only use unique
                # TODO - too slow
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
        self.bulk = self.datas.bulk()
        self.indexs.reindex(self.bulk.all())

    # @staticmethod
    # def make_key(vals):
    #     assert isinstance(vals, dict, 'make_key vals invaild')
    #     keys = [vals.keys()]
    #     keys.sort()
    #     _str = '_'.join(reduce(lambda x, y: x + [y, vals[y]], keys, []))
    #     _str = _str.encode("latin-1")
    #     return hashlib.md5(_str).hexdigest()
