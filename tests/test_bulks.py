# -*- coding: utf-8 -*-

import os
import time
import unittest
import asyncio
import shutil
from tinydb import TinyDB, where
from tinydb.storages import MemoryStorage

try:
    from tinydb_bulk.bulk import TableBulk
    from tinydb_bulk.indexs import IndexModel, ASCENDING, DESCENDING, HASHED
except ImportError:
    import sys
    sys.path.insert(0, os.getcwd())
    from tinydb_bulk.bulk import TableBulk
    from tinydb_bulk.indexs import IndexModel, ASCENDING, DESCENDING, HASHED

LOOP = asyncio.get_event_loop()
LOGS = []
PYPATH = os.path.abspath(os.path.dirname(__file__))


def async_test(f):
    def wrapper(*args, **kwargs):
        coro = asyncio.coroutine(f)
        future = coro(*args, **kwargs)
        LOOP.run_until_complete(future)
    return wrapper


class TestMathFunc(unittest.TestCase):
    """Test mathfuc.py"""

    RANGE_COUNT = 500
    TEST_JSON_PATH = os.path.join(PYPATH, 'test.json')

    def upsert_tinydb_test(self, db, _type):
        start = time.time()
        table = db.table('test')

        for i in range(self.RANGE_COUNT):
            table.upsert(
                {'i': i, 'a': i % 2, 'b': i * 2},
                where('i') == i
            )

        interval = time.time() - start
        print('tinydb {} upsert: {} sec'.format(_type, interval))
        db.close()

    def upsert_bulk_test(self, db, _type):
        start = time.time()
        index_one = IndexModel([
            ('i', HASHED),
        ], True)

        index_two = IndexModel([
            ('a', ASCENDING),
            ('b', DESCENDING),
        ])

        # db = TinyDB(storage=MemoryStorage)
        table = TableBulk([index_one, index_two], db.table('test'))

        for i in range(self.RANGE_COUNT):
            table.upsert_one(
                {'i': i},
                {'$set': {'i': i, 'a': i % 2, 'b': i * 2}}
            )

        interval = time.time() - start
        print('bulk {} upsert: {} sec'.format(_type, interval))

        table.flush()
        interval = time.time() - start
        print('bulk {} upsert flush: {} sec'.format(_type, interval))
        db.close()

    def test_aa_upsert_memory(self):
        with TinyDB(storage=MemoryStorage) as db:
            self.upsert_tinydb_test(db, 'memory')

    def test_ab_upsert_memory(self):
        with TinyDB(storage=MemoryStorage) as db:
            self.upsert_bulk_test(db, 'json')

    def test_ba_upsert_json(self):
        try:
            os.remove(self.TEST_JSON_PATH)
        except Exception as ex:
            print(ex)

        with TinyDB(self.TEST_JSON_PATH) as db:
            self.upsert_tinydb_test(db, 'json')

    def test_bb_upsert_json(self):
        try:
            os.remove(self.TEST_JSON_PATH)
        except Exception as ex:
            print(ex)

        with TinyDB(self.TEST_JSON_PATH) as db:
            self.upsert_bulk_test(db, 'json')


if __name__ == '__main__':
    unittest.main()
