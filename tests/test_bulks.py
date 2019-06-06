# -*- coding: utf-8 -*-

import os
import time
import unittest
import asyncio
import shutil
from collections import defaultdict
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

    RANGE_COUNT = 1000
    TEST_JSON_PATH = os.path.join(PYPATH, 'test.json')

    # ----------------------------------------------
    #        real test
    # ----------------------------------------------

    def test_bulk(self):
        start = time.time()
        index_one = IndexModel([
            ('i', HASHED),
        ], True)

        # index_two = IndexModel([
        #     ('a', ASCENDING),
        #     ('b', DESCENDING),
        # ])
        try:
            os.remove('./tests/test.json')
        except Exception:
            pass

        _type = 'json'
        db = TinyDB('./tests/test.json')
        table = TableBulk([index_one], db.table('test'))
        start = time.time()

        for i in range(self.RANGE_COUNT):
            table.upsert_one(
                {'i': i},
                {'$set': {'a': i % 2, 'b': i * 2}}
            )

        interval = time.time() - start
        start2 = time.time()
        # table.flush()
        interval2 = time.time() - start2
        interval3 = time.time() - start
        print('bulk {} upsert: {} flush: {} totle {} sec'.format(
            _type, interval, interval2, interval3))
        db.close()

    def test_z_dict(self):
        start = time.time()
        _type = 'dict'
        docs = defaultdict(dict)
        for i in range(self.RANGE_COUNT):
            docs[i].update({'i': i, 'a': i % 2, 'b': i * 2})

        interval = time.time() - start
        start2 = time.time()
        # table.flush()
        interval2 = time.time() - start2
        interval3 = time.time() - start
        print('bulk {} upsert: {} flush: {} totle {} sec'.format(
            _type, interval, interval2, interval3))

    # def test_bulk(self):
    #     start = time.time()
    #     index_one = IndexModel([
    #         ('i', HASHED),
    #     ], True)

    #     index_two = IndexModel([
    #         ('a', ASCENDING),
    #         ('b', DESCENDING),
    #     ])

    #     _type = 'json'
    #     db = TinyDB('./tests/test.json')
    #     table = TableBulk([index_one, index_two], db.table('test'))
    #     from collections import defaultdict
    #     docs = defaultdict(dict)
    #     for i in range(self.RANGE_COUNT):
    #         docs[i].update({'i': i, 'a': i % 2, 'b': i * 2})

    #     interval = time.time() - start
    #     print('bulk {} upsert: {} sec'.format(_type, interval))

    #     table.flush()
    #     interval = time.time() - start
    #     print('bulk {} upsert flush: {} sec'.format(_type, interval))
    #     db.close()


if __name__ == '__main__':
    unittest.main()
