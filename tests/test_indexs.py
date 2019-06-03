# -*- coding: utf-8 -*-

import os
import unittest
import asyncio
import shutil
from tinydb import TinyDB
from tinydb.storages import MemoryStorage

try:
    from tinydb_bulk.indexs import IndexsMrg
    from tinydb_bulk.indexs import IndexTable, DuplicateError
    from tinydb_bulk.indexs import IndexModel, ASCENDING, DESCENDING, HASHED
except ImportError:
    import sys
    sys.path.insert(0, os.getcwd())
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
        try:
            it_one.upsert_one(1, {'a': 1, 'b': 2, 'i': 3})
        except Exception as ex:
            self.assertIsInstance(ex, DuplicateError)

        it_two = IndexTable(index_two)
        it_two.upsert_one(1, {'a': 1, 'b': 2, 'i': 3})
        it_two.upsert_one(1, {'a': 1, 'b': 2, 'i': 3})

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
            it_one.upsert_one(1, {'a': 1, 'b': 2, 'i': 3})
        except Exception as ex:
            self.assertIsInstance(ex, DuplicateError)


if __name__ == '__main__':
    unittest.main()
