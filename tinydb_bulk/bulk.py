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

        # TODO  - support find command
        _filter = mop.conv_doc2root(_filter)

        ids = self.indexs.find_hash_key(_filter)
        datas = [
            mop.conv_root2doc(self.datas.get(doc_id=_id._id))
            for _id in ids
        ]
        return datas

    def find_one(self, _filter):
        if not isinstance(_filter, dict):
            raise InputError('_filter invaild')

        # TODO  - support find command
        _filter = mop.conv_doc2root(_filter)

        ids = self.indexs.find_hash_key(_filter)
        if not ids:
            return None

        data = self.datas.get(doc_id=list(ids)[0]._id)
        return mop.conv_root2doc(data)

    def __find_one(self, _filter):
        # # TODO  - support find command
        # _filter = mop.conv_doc2root(_filter)

        # find index
        ids = self.indexs.find_hash_key(_filter)
        if not ids:
            return None

        ids = list(ids)
        data = self.datas.get(doc_id=ids[0]._id)
        return ids[0]._id, data

    def upsert_one(self, _filter, update):
        if not isinstance(_filter, dict):
            raise InputError('_filter invaild')

        if not isinstance(update, dict):
            raise InputError('_filter invaild')

        # TODO  - support find command
        _filter = mop.conv_doc2root(_filter)

        # is indexs not same
        if self.indexs.is_need_reindex():
            self.reset()
            raise IndexExpiredError('index expired')

        data = self.__find_one(_filter)
        if not data:
            _id = None
            data = {}
        else:
            _id = data[0]
            data = data[1]

        if '$setOnInsert' in update and not data:
            setoninsert = mop.conv_doc2root(update['$setOnInsert'])
            data.update(setoninsert)
            data.update(_filter)

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
