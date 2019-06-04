# tinydb_bulk

## tinydb_bulk test

```shell
python demo/demo_print_bulk.py

tinydb memory upsert: 1.7379908561706543 sec
bulk memory upsert: 0.6560075283050537 sec
bulk memory upsert flush: 0.7590103149414062 sec
tinydb json upsert: 29.756213665008545 sec
bulk json upsert: 0.6801221370697021 sec
bulk json upsert flush: 0.8031060695648193 sec
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
