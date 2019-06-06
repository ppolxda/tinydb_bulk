# tinydb_bulk

## tinydb_bulk test

```bash
REM 'upsert 1000 count'
python demo/demo_print_bulk.py

bulk memory upsert: 0.04400014877319336 sec
bulk memory upsert flush: 0.05500054359436035 sec
bulk json upsert: 0.04799938201904297 sec
bulk json upsert flush: 0.06200003623962402 sec
```

## tinydb_bulk demo

```python
from tinydb import TinyDB, where
from tinydb.storages import MemoryStorage
from tinydb_bulk import TableBulk
from tinydb_bulk import IndexModel, HASHED, ASCENDING, DESCENDING


def main():
    index_one = IndexModel([
        ('i', HASHED),
    ], True)

    index_two = IndexModel([
        ('a', ASCENDING),
        ('b', DESCENDING),
    ])

    with TinyDB(storage=MemoryStorage) as db:
        table = TableBulk([index_one, index_two], db.table('test'))

        for i in range(100):
            table.upsert_one(
                {'i': i},
                {'$set': {'i': i, 'a': i % 2, 'b': i * 2}}
            )

        table.flush()



if __name__ == '__main__':
    main()
```
