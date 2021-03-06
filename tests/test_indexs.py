# -*- coding: utf-8 -*-

import os
import unittest
import asyncio
import shutil
from tinydb import TinyDB
from tinydb.storages import MemoryStorage

try:
    from tinydb_bulk.indexs import IndexHint
    from tinydb_bulk.indexs import EnumStage
    from tinydb_bulk.indexs import IndexsMrg
    from tinydb_bulk.indexs import IndexTable, DuplicateError
    from tinydb_bulk.indexs import IndexModel, ASCENDING, DESCENDING, HASHED
except ImportError:
    import sys
    sys.path.insert(0, os.getcwd())
    from tinydb_bulk.indexs import IndexHint
    from tinydb_bulk.indexs import EnumStage
    from tinydb_bulk.indexs import IndexsMrg
    from tinydb_bulk.indexs import IndexTable, DuplicateError
    from tinydb_bulk.indexs import IndexModel, ASCENDING, DESCENDING, HASHED

LOOP = asyncio.get_event_loop()
LOGS = []


def async_test(f):
    def wrapper(*args, **kwargs):
        coro = asyncio.coroutine(f)
        future = coro(*args, **kwargs)
        LOOP.run_until_complete(future)
    return wrapper


class TestMathFunc(unittest.TestCase):
    """Test mathfuc.py"""

    def test_a_indexs(self):
        index_one = IndexModel([
            ('i', HASHED),
        ], True)

        index_two = IndexModel([
            ('a', ASCENDING),
            ('b', DESCENDING),
        ])

        # db = TinyDB(storage=MemoryStorage)
        it_one = IndexTable(index_one)
        it_one.upsert_one(1, {'a': 1, 'b': 2, 'i': 3})
        it_one.upsert_one(1, {'a': 1, 'b': 2, 'i': 5})
        try:
            it_one.upsert_one(2, {'a': 1, 'b': 2, 'i': 5})
        except Exception as ex:
            self.assertIsInstance(ex, DuplicateError)
        else:
            self.assertTrue(False)

        it_two = IndexTable(index_two)
        it_two.upsert_one(1, {'a': 1, 'b': 2, 'i': 3})
        it_two.upsert_one(1, {'a': 1, 'b': 2, 'i': 5})

    def test_b_indexs(self):
        index_one = IndexModel([
            ('i', HASHED),
        ], True)

        index_two = IndexModel([
            ('a', ASCENDING),
            ('b', DESCENDING),
        ])

        it_one = IndexsMrg([index_one, index_two])
        it_one.upsert_one(1, {'a': 1, 'b': 2, 'i': 3})
        try:
            it_one.upsert_one(2, {'a': 1, 'b': 2, 'i': 3})
        except Exception as ex:
            self.assertIsInstance(ex, DuplicateError)
        else:
            self.assertTrue(False)

    def test_c_indexs(self):
        index_one = IndexModel([
            ('i', HASHED),
        ], True)

        index_two = IndexModel([
            ('a', ASCENDING),
            ('b', DESCENDING),
            ('c', DESCENDING),
        ])

        it_one = IndexsMrg([index_one, index_two])

        for i in range(1000):
            it_one.upsert_one(
                i, {'i': i, 'a': i % 10, 'b': i * 2, 'c': i % 2}
            )

        result = it_one.find_hash_fetch({'a': 2, 'b': 4})
        result = list(result)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].doc['a'], 2)
        self.assertEqual(result[0].doc['b'], 4)

        result = it_one.find_hash_fetch({'a': 2, 'c': 0})
        result = list(result)
        self.assertEqual(len(result), 100)
        self.assertEqual(result[0].doc['a'], 2)
        self.assertEqual(result[0].doc['c'], 0)

        result = it_one.find_hash_fetch({'a': 2, 'c': 1})
        result = list(result)
        self.assertEqual(len(result), 0)

    def test_d_explain(self):
        index_one = IndexModel([
            ('i', HASHED),
        ], True)

        it_one = IndexsMrg([index_one])

        for i in range(1000):
            it_one.upsert_one(
                i, {'i': i, 'a': i % 10, 'b': i * 2, 'c': i % 2}
            )

        explain = it_one.explain(['i'])
        self.assertIsInstance(explain, list)
        self.assertEqual(len(explain), 1)
        self.assertIsInstance(explain[0], IndexHint)
        self.assertEqual(explain[0].keys, ['i'])
        self.assertEqual(explain[0].stage, EnumStage.IXSCAN)

        explain = it_one.explain(['i', 'a'])
        self.assertIsInstance(explain, list)
        self.assertEqual(len(explain), 2)
        self.assertIsInstance(explain[0], IndexHint)
        self.assertEqual(explain[0].keys, ['i'])
        self.assertEqual(explain[0].stage, EnumStage.IXSCAN)
        self.assertEqual(explain[1].keys, ['a'])
        self.assertEqual(explain[1].stage, EnumStage.FETCH)

    def test_c_find_hash(self):
        index_one = IndexModel([
            ('i', HASHED),
        ], True)

        index_two = IndexModel([
            ('a', ASCENDING),
            ('b', DESCENDING),
            ('c', DESCENDING),
        ])

        it_one = IndexsMrg([index_one, index_two])

        for i in range(1000):
            it_one.upsert_one(
                i, {'i': i, 'a': i % 10, 'b': i * 2, 'c': i % 2}
            )

        result = it_one.find_hash({'a': 2, 'b': 4})
        result = list(result)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].doc['a'], 2)
        self.assertEqual(result[0].doc['b'], 4)

        result = it_one.find_hash({'a': 2, 'c': 0})
        result = list(result)
        self.assertEqual(len(result), 100)
        self.assertEqual(result[0].doc['a'], 2)
        self.assertEqual(result[0].doc['c'], 0)

        result = it_one.find_hash({'a': 2, 'c': 1})
        result = list(result)
        self.assertEqual(len(result), 0)

    # def test_e_hash_indexs(self):
    #     index_one = IndexModel([
    #         ('i', HASHED),
    #     ], True)

    #     it_one = IndexsMrg([index_one])

    #     for i in range(1000):
    #         it_one.upsert_one(
    #             i, {'i': i, 'a': i % 10, 'b': i * 2, 'c': i % 2}
    #         )

    #     ids = it_one.find_hash({'i': 1})
    #     self.assertIsNotNone(ids)
    #     self.assertIsInstance(ids, set)
    #     ids = next(iter(ids))
    #     self.assertEqual(ids.doc['i'], 1)


if __name__ == '__main__':
    unittest.main()
