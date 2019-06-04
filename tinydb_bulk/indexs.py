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


class Document(object):

    def __init__(self, _id, doc):
        self._id = _id
        self.doc = doc
        self.hash = self.__hash_doc(self.doc)

    def to_dict(self):
        return {
            '_id': self._id,
            'doc': self.doc,
        }

    def __hash__(self):
        return hash(self.hash)

    def __hash_doc(self, doc):
        return hashlib.md5(six.b(
            json.dumps(doc, sort_keys=True)
        )).hexdigest()


class IndexModel(object):

    def __init__(self, indexs, unique=False):
        self.indexs = indexs
        self.unique = unique

        keys = set()
        sort_types = set()
        for i in self.indexs:
            if not isinstance(i, (tuple, list)) or \
                    not isinstance(i[0], six.string_types) or \
                    i[1] not in INDEX_FLAGS or \
                    i[0] in keys:
                raise InputError('IndexModel index invaild')

            keys.add(i[0])
            sort_types.add(i[1])

        if HASHED in sort_types and len(sort_types) != 1:
            raise InputError('HASHED IndexModel invaild')

        self.keys = keys
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
        self.indexs_uniques = set()

    def clear(self):
        self.index_log = deque(maxlen=50)
        self.indexs_hashed = defaultdict(dict)
        self.indexs_uniques = set()

    def size(self):
        return len(self.indexs_hashed['_id'])

    def has_key(self, key):
        return key in self.index_model.keys

    def is_empty(self):
        return self.size() <= 0

    def upsert_one(self, _id, doc):
        # TODO - support array index, dict index
        new_doc = {key: doc.get(key, None) for key in self.index_model.keys
                   if not isinstance(doc.get(key, None), (list, dict))}
        new_doc = Document(_id, new_doc)

        # check unique
        if self.index_model.unique and new_doc.hash in self.indexs_uniques:
            raise DuplicateError('doc duplicate [{}][key {}]'.format(
                self.index_model.name, new_doc.doc))

        self.indexs_uniques.add(new_doc.hash)

        # TODO - SORT ASCENDING, DESCENDING
        if self.indexs_hashed:
            old_doc = self.indexs_hashed['_id'].get(_id, None)
            if old_doc and new_doc.hash != old_doc.hash:
                for key, val in old_doc.items():
                    if val not in self.indexs_hashed[key]:
                        self.indexs_hashed[key][val] = set()

                    self.indexs_hashed[key][val].remove(old_doc)

        for key, val in new_doc.doc.items():
            if val not in self.indexs_hashed[key]:
                self.indexs_hashed[key][val] = set()

            self.indexs_hashed[key][val].add(new_doc)

        # log upsert_one，to cmp index
        self.index_log.append({'op': 'upsert_one', 'doc': doc})

    def find_hash_key(self, key, val):
        if key not in self.index_model.keys:
            return set()

        return self.indexs_hashed[key].get(val, set())

    def reindex(self, docs):
        self.clear()
        for doc in docs:
            self.upsert_one(doc.doc_id, doc)


class IndexsMrg(object):

    def __init__(self, indexs):
        self.indexs = indexs
        for i in self.indexs:
            if not isinstance(i, IndexModel):
                raise InputError('indexs invaild')

        # self.datas = db.table('datas')
        self.indexs_tables = [IndexTable(i) for i in self.indexs]
        self.indexs_size = len(self.indexs_tables)

    def is_need_reindex(self):
        # size check
        size_check = set(i.size() for i in self.indexs_tables)
        if len(size_check) != 1:
            return True

        # log check
        _begin = self.indexs_tables[0].index_log
        for i in self.indexs_tables:
            if _begin != i.index_log:
                return True
        return False

    def get_index(self, index=0):
        if index >= self.indexs_size:
            raise InputError('indexs invaild')
        return self.indexs_tables[index]

    def is_empty(self, index=0):
        _i = self.get_index(index)
        return _i.is_empty(0)

    def clear(self):
        for i in self.indexs_tables:
            i.clear()

    def upsert_one(self, _id, doc):
        doc = conv_doc2root(doc)
        for i in self.indexs_tables:
            i.upsert_one(_id, doc)

    def find_hash_key(self, _doc_keys):
        result = []
        for key, val in _doc_keys.items():

            for i in self.indexs_tables:
                r = i.find_hash_key(key, val)
                if not r:
                    continue

                result.append(r)
                break

        return reduce(lambda x, y: y if x is None else x & y, result, None)

    def reindex(self, docs):
        self.clear()
        for doc in docs:
            self.upsert_one(doc.doc_id, doc)
