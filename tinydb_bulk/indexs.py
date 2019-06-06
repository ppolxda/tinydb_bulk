# -*- coding: utf-8 -*-
"""
@create: 2019-05-31 00:33:49.

@author: ppolxda

@desc:
"""
import six
# import bisect
import hashlib
from collections import deque
from collections import defaultdict
from collections import OrderedDict
from tinydb import where
from tinydb.database import TinyDB
from tinydb.database import Table
from .mongo_op import conv_doc2root
from .errors import Error
from .errors import InputError
from .errors import DuplicateError
try:
    import ujson as json
except ImportError:
    import json


reduce = six.moves.reduce
HASHED = 'hashed'
ASCENDING = 1
DESCENDING = -1
INDEX_FLAGS = [HASHED, ASCENDING, DESCENDING]
INDEX_SORT_FLAGS = [ASCENDING, DESCENDING]
_ASCENDING = str(ASCENDING)
_DESCENDING = str(DESCENDING)


class EnumStage(object):
    """类功能[枚举]."""

    COLLSCAN = 'COLLSCAN'  # for a collection scan
    IXSCAN = 'IXSCAN'  # for scanning index keys
    FETCH = 'FETCH'  # for retrieving documents
    SHARD_MERGE = 'SHARD_MERGE'  # for merging results from shards
    # for filtering out orphan documents from shards
    SHARDING_FILTER = 'SHARDING_FILTER'

    enum_list = [
        COLLSCAN,
        IXSCAN,
        FETCH,
        SHARD_MERGE,
        SHARDING_FILTER,
    ]

    @classmethod
    def is_invaild(cls, val):
        """是否无效枚举."""
        return val not in cls.enum_list


class IndexHint(object):

    def __init__(self, keys, inum, stage):
        self.keys = keys
        self.inum = inum
        self.stage = stage

    def __len__(self):
        pass


COLLSCAN_HINT = IndexHint(None, -1, EnumStage.COLLSCAN)


class Document(object):

    def __init__(self, _id, doc):
        # assert isinstance(doc, OrderedDict)
        self._id = _id
        self.doc = doc
        self.hash = self.__hash_doc(self.doc)

    # def to_dict(self):
    #     return {
    #         '_id': self._id,
    #         'doc': self.doc,
    #     }

    def __hash__(self):
        return self.hash

    @staticmethod
    def __hash_doc(doc):
        return hash(frozenset(doc.items()))


class IndexModel(object):

    def __init__(self, indexs, unique=False):
        self.indexs = indexs
        self.unique = unique

        keys = set()
        keys_sort = []
        sort_types = set()
        for i in self.indexs:
            if not isinstance(i, (tuple, list)) or \
                    not isinstance(i[0], six.string_types) or \
                    i[1] not in INDEX_FLAGS or \
                    i[0] in keys:
                raise InputError('IndexModel index invaild')

            keys.add(i[0])
            keys_sort.append(i[0])
            sort_types.add(i[1])

        if HASHED in sort_types and len(sort_types) != 1:
            raise InputError('HASHED IndexModel invaild')

        self.keys = keys
        self.keys_sort = keys_sort
        self.name = '_'.join(
            reduce(lambda x, y: x + [y[0], str(y[1])], self.indexs, [])
        )
        self.indexs_keys = {i[0]: i[1] for i in self.indexs}
        self.indexs_names = ['_'.join([i[0], str(i[1])]) for i in self.indexs]


class IndexTable(object):

    def __init__(self, index_model: IndexModel):
        assert isinstance(index_model, IndexModel)
        self.index_model = index_model
        self.index_log = deque(maxlen=50)
        self.indexs_hashed = defaultdict(dict)

    def create_doc(self, _id, doc):
        new_doc = {
            key: doc.get(key, None)
            for key in self.index_model.keys_sort
            if not isinstance(doc.get(key, None), (list, dict))
        }
        return Document(_id, new_doc)

    def clear(self):
        self.index_log = deque(maxlen=50)
        self.indexs_hashed = defaultdict(dict)

    def size(self):
        return len(self.indexs_hashed['_id'])

    def is_empty(self):
        return self.size() <= 0

    def is_duplicate(self, _id, doc):
        if not self.index_model.unique:
            return False

        if isinstance(doc, Document):
            new_doc = doc
        else:
            new_doc = self.create_doc(_id, doc)

        has_hash = self.indexs_hashed['_hash'].get(new_doc.hash, None)
        return has_hash and next(iter(has_hash))._id != _id

    def upsert_one(self, _id, doc):
        # TODO - support array index, dict index
        new_doc = self.create_doc(_id, doc)

        # check unique
        if self.is_duplicate(_id, new_doc):
            raise DuplicateError('doc duplicate [{}][key {}]'.format(
                self.index_model.name, doc))

        # TODO - SORT ASCENDING, DESCENDING
        self._upsert_doc(new_doc)
        # log upsert_one，to cmp index
        # TODO - log too slow
        # self.index_log.append({'op': 'upsert_one', 'doc': doc})

    def hint_keys(self, keys):
        result = []
        for key in self.index_model.keys_sort:
            if key in keys:
                result.append(key)
            else:
                break
        return result

    def find_hash_ixscan(self, _filter, keys):
        _keys = '_'.join(keys)
        _val = '_'.join([str(_filter[key]) for key in keys])
        return self.indexs_hashed[_keys].get(_val, None)

    def find_hash_fetch(self, _filter, keys):
        ids = []
        for key in keys:
            val = _filter[key]
            r = self.find_hash_key(key, val)
            if not r:
                continue

            ids.append(r)
            break

        return reduce(lambda x, y: y if x is None else x & y,
                      filter(lambda x: x, ids), None)

    def find_hash_key(self, key, val):
        if key not in self.index_model.keys:
            raise InputError('find_hash_key key not in index')

        return self.indexs_hashed[key].get(val, set())

    def reindex(self, docs):
        self.clear()
        for doc in docs:
            self.upsert_one(doc.doc_id, doc)

    def _add_indexs(self, key, val, doc):
        if val not in self.indexs_hashed[key]:
            self.indexs_hashed[key][val] = set()

        self.indexs_hashed[key][val].add(doc)

    def _remove_indexs(self, key, val, doc):
        if val not in self.indexs_hashed[key]:
            self.indexs_hashed[key][val] = set()

        self.indexs_hashed[key][val].remove(doc)

        if not self.indexs_hashed[key][val]:
            self.indexs_hashed[key].pop(val)

    def _upsert_doc(self, new_doc):
        add_doc = True
        while True:
            if not self.indexs_hashed:
                break

            old_doc = self.indexs_hashed['_id'].get(new_doc._id, None)
            if not old_doc:
                break

            old_doc = next(iter(old_doc))
            if new_doc.hash == old_doc.hash:
                add_doc = False
                break

            # _id must only one
            self._remove_doc(old_doc)

        if add_doc:
            self._add_doc(new_doc)

    def _add_doc(self, new_doc):
        keys = []
        vals = []
        for key in self.index_model.keys_sort:
            val = new_doc.doc[key]
            keys.append(key)
            vals.append(str(val))
            self._add_indexs(
                '_'.join(keys),
                '_'.join(vals),
                new_doc
            )
            self._add_indexs(key, val, new_doc)

        # add new index
        self._add_indexs('_id', new_doc._id, new_doc)
        self._add_indexs('_hash', new_doc.hash, new_doc)

    def _remove_doc(self, old_doc):
        keys = []
        vals = []
        for key in self.index_model.keys_sort:
            val = old_doc.doc[key]
            keys.append(key)
            vals.append(str(val))

            self._remove_indexs(
                '_'.join(keys),
                '_'.join(vals),
                old_doc
            )
            self._remove_indexs(key, val, old_doc)

        self._remove_indexs('_id', old_doc._id, old_doc)
        self._remove_indexs('_hash', old_doc.hash, old_doc)


class IndexsMrg(object):

    def __init__(self, indexs):
        self.indexs = indexs
        for i in self.indexs:
            if not isinstance(i, IndexModel):
                raise InputError('indexs invaild')

        # self.datas = db.table('datas')
        self.indexs_tables = [IndexTable(i) for i in self.indexs]
        self.indexs_size = len(self.indexs_tables)
        self.explain_cache = {}

    def is_need_reindex(self):
        # size check
        size_check = set(i.size() for i in self.indexs_tables)
        if len(size_check) != 1:
            return True

        # TODO - log too slow
        # log check
        # _begin = self.indexs_tables[0].index_log
        # for i in self.indexs_tables:
        #     if _begin != i.index_log:
        #         return True
        return False

    def get_index(self, index=0):
        if index >= self.indexs_size:
            raise InputError('indexs invaild')
        return self.indexs_tables[index]

    def is_empty(self, index=0):
        _i = self.get_index(index)
        return _i.is_empty(0)

    def is_duplicate(self, _id, doc):
        for i in self.indexs_tables:
            if i.is_duplicate():
                return True
        return False

    def clear(self):
        for i in self.indexs_tables:
            i.clear()

    def upsert_one(self, _id, doc):
        doc = conv_doc2root(doc)
        for i in self.indexs_tables:
            i.upsert_one(_id, doc)

    def _find_explain(self, keys, find_sort=None):
        '''
        COLLSCAN for a collection scan
        IXSCAN for scanning index keys
        FETCH for retrieving documents
        SHARD_MERGE for merging results from shards
        SHARDING_FILTER for filtering out orphan documents from shards
        '''
        if find_sort is None:
            find_sort = []

        indexhints = list(filter(
            lambda x: x.keys,
            map(
                lambda i: IndexHint(
                    i[1].hint_keys(keys), i[0],
                    EnumStage.IXSCAN
                ),
                enumerate(self.indexs_tables))
        ))

        # not hint any
        if not indexhints:
            if find_sort:
                find_sort.append(IndexHint(keys, -1, EnumStage.FETCH))
            else:
                find_sort.append(COLLSCAN_HINT)
            return find_sort

        # find max hit
        index = max(indexhints)
        find_sort.append(index)

        # check finish
        diff = set(keys) - set(index.keys)
        if diff:
            self._find_explain(list(diff), find_sort)
        return find_sort

    def explain(self, keys):
        assert isinstance(keys, list)
        keys_str = '_'.join(keys)
        if keys_str in self.explain_cache:
            return self.explain_cache[keys_str]

        query_plain = self._find_explain(keys)
        self.explain_cache[keys_str] = query_plain
        return query_plain

    def find_hash(self, _doc_keys):
        keys = list(_doc_keys.keys())
        explain = self.explain(keys)
        ids = []

        for plain in explain:
            index = self.indexs_tables[plain.inum]

            if plain.stage == EnumStage.IXSCAN:
                ids.append(index.find_hash_ixscan(_doc_keys, plain.keys))
            elif plain.stage == EnumStage.FETCH:
                ids.append(index.find_hash_fetch(_doc_keys, plain.keys))
            else:
                ids.append(self.find_hash_fetch(_doc_keys))

        if len(ids) == 1:
            return next(iter(ids))
        else:
            return reduce(lambda x, y: y if x is None else x & y,
                          filter(lambda x: x, ids), None)

    def find_hash_fetch(self, _doc_keys):
        result = []
        for key, val in _doc_keys.items():

            for i in self.indexs_tables:
                if key not in i.index_model.keys:
                    continue

                r = i.find_hash_key(key, val)
                if not r:
                    continue

                result.append(r)
                break

        return reduce(lambda x, y: y if x is None else x & y,
                      filter(lambda x: x, result), None)

    def reindex(self, docs):
        self.clear()
        for doc in docs:
            self.upsert_one(doc.doc_id, doc)
