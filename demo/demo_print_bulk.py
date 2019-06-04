# -*- coding: utf-8 -*-
import os
import time
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

PYPATH = os.path.abspath(os.path.dirname(__file__))


RANGE_COUNT = 500
TEST_JSON_PATH = os.path.join(PYPATH, 'test.json')


def upsert_tinydb_test(db, _type):
    start = time.time()
    table = db.table('test')

    for i in range(RANGE_COUNT):
        table.upsert(
            {'i': i, 'a': i % 2, 'b': i * 2},
            where('i') == i
        )

    interval = time.time() - start
    print('tinydb {} upsert: {} sec'.format(_type, interval))
    db.close()


def upsert_bulk_test(db, _type):
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

    for i in range(RANGE_COUNT):
        table.upsert_one(
            {'i': i}, {'$set': {'i': i, 'a': i % 2, 'b': i * 2}}
        )

    interval = time.time() - start
    print('bulk {} upsert: {} sec'.format(_type, interval))

    table.flush()
    interval = time.time() - start
    print('bulk {} upsert flush: {} sec'.format(_type, interval))
    db.close()


def upsert_memory():
    with TinyDB(storage=MemoryStorage) as db:
        upsert_tinydb_test(db, 'memory')


def upser_bulk_memory():
    with TinyDB(storage=MemoryStorage) as db:
        upsert_bulk_test(db, 'memory')


def upsert_json():
    try:
        os.remove(TEST_JSON_PATH)
    except Exception:
        pass

    with TinyDB(TEST_JSON_PATH) as db:
        upsert_tinydb_test(db, 'json')


def upsert_bulk_json():
    try:
        os.remove(TEST_JSON_PATH)
    except Exception:
        pass

    with TinyDB(TEST_JSON_PATH) as db:
        upsert_bulk_test(db, 'json')


def main():
    for func in [
        upsert_memory,
        upser_bulk_memory,
        upsert_json,
        upsert_bulk_json
    ]:
        func()


if __name__ == '__main__':
    main()
