# -*- coding: utf-8 -*-

import os
import time
import unittest
import shutil
from tinydb import TinyDB, where
from tinydb.storages import MemoryStorage

try:
    from tinydb_bulk import mongo_op as mp
except ImportError:
    import sys
    sys.path.insert(0, os.getcwd())
    from tinydb_bulk import mongo_op as mp

PYPATH = os.path.abspath(os.path.dirname(__file__))


class TestMathFunc(unittest.TestCase):
    """Test mathfuc.py"""

    def test_a_conv_doc2root(self):
        doc = {'a': 1, 'b': 2, 'c': 3}
        new_doc = mp.conv_doc2root(doc)
        self.assertEqual(doc, new_doc)

        doc = {'a': 1, 'b': 2, 'c': {'d': 3, 'f': 4, 'g': {'h': 5}}}
        real_doc = {'a': 1, 'b': 2, 'c.d': 3, 'c.f': 4, 'c.g.h': 5}
        new_doc = mp.conv_doc2root(doc)
        self.assertNotEqual(doc, new_doc)
        self.assertEqual(real_doc, new_doc)

        new_doc = mp.conv_root2doc(real_doc)
        self.assertEqual(doc, new_doc)

    def test_z_op(self):
        doc = {}
        mp.op_inc(doc, {'i': 1})
        self.assertEqual(doc['i'], 1)
        mp.op_inc(doc, {'i': 1})
        self.assertEqual(doc['i'], 2)
        mp.op_inc(doc, {'i': 3})
        self.assertEqual(doc['i'], 5)

        mp.op_max(doc, {'h': 5})
        self.assertEqual(doc['h'], 5)
        mp.op_max(doc, {'h': 6})
        self.assertEqual(doc['h'], 6)
        mp.op_max(doc, {'h': 3})
        self.assertEqual(doc['h'], 6)

        mp.op_min(doc, {'j': 5})
        self.assertEqual(doc['j'], 5)
        mp.op_min(doc, {'j': 6})
        self.assertEqual(doc['j'], 5)
        mp.op_min(doc, {'j': 3})
        self.assertEqual(doc['j'], 3)

        mp.op_unset(doc, {'j': ''})
        self.assertNotIn('j', doc)
        mp.op_unset(doc, {'j': ''})

        mp.op_rename(doc, {'h': 'j'})
        self.assertIn('j', doc)
        self.assertNotIn('h', doc)
        self.assertEqual(doc['j'], 6)

    def test_z_op_addtoset(self):
        doc = {'a': 1, 'b': 2, 'c': 3}
        mp.op_addtoset(doc, {'f': 100})
        self.assertEqual(len(doc['f']), 1)
        self.assertTrue(100 in doc['f'])
        mp.op_addtoset(doc, {'f': 100})
        self.assertEqual(len(doc['f']), 1)
        self.assertTrue(100 in doc['f'])

        mp.op_addtoset(doc, {'f': 101})
        self.assertEqual(len(doc['f']), 2)
        self.assertTrue(100 in doc['f'])
        self.assertTrue(101 in doc['f'])

        mp.op_addtoset(doc, {'f': {'$each': [100, 101, 102]}})
        self.assertEqual(len(doc['f']), 3)
        self.assertTrue(100 in doc['f'])
        self.assertTrue(101 in doc['f'])
        self.assertTrue(102 in doc['f'])

        mp.op_addtoset(doc, {'f.h': {'$each': [100, 101, 102]}})
        self.assertEqual(len(doc['f.h']), 3)
        self.assertTrue(100 in doc['f.h'])
        self.assertTrue(101 in doc['f.h'])
        self.assertTrue(102 in doc['f.h'])


if __name__ == '__main__':
    unittest.main()
