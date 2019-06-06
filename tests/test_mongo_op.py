# -*- coding: utf-8 -*-

import os
import time
import unittest
import shutil
import datetime
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

        try:
            new_doc = mp.conv_root2doc('asdasdas')
        except Exception as ex:
            self.assertIsInstance(ex, mp.InputError)
            self.assertTrue(str(ex).find('conv_root2doc doc invaild') >= 0)
        else:
            self.assertTrue(False)

        try:
            new_doc = mp.conv_doc2root('asdasdas')
        except Exception as ex:
            self.assertIsInstance(ex, mp.InputError)
            self.assertTrue(str(ex).find('conv_doc2root doc invaild') >= 0)
        else:
            self.assertTrue(False)

        try:
            new_doc = mp.conv_doc2root({'$asd': ''})
        except Exception as ex:
            self.assertIsInstance(ex, mp.InputError)
            self.assertTrue(str(ex).find(
                'conv_doc2root not support op conv') >= 0)
        else:
            self.assertTrue(False)

    def test_z_op_set(self):
        doc = {}
        mp.op_set(doc, {'a': 5})
        self.assertEqual(doc['a'], 5)
        mp.op_set(doc, {'a': 6})
        self.assertEqual(doc['a'], 6)
        mp.op_set(doc, {'a': 3})
        self.assertEqual(doc['a'], 3)

    def test_z_op_inc(self):
        doc = {}
        mp.op_inc(doc, {'i': 1})
        self.assertEqual(doc['i'], 1)
        mp.op_inc(doc, {'i': 1})
        self.assertEqual(doc['i'], 2)
        mp.op_inc(doc, {'i': 3})
        self.assertEqual(doc['i'], 5)

        mp.op_set(doc, {'ii': 'asdasd'})
        try:
            mp.op_inc(doc, {'ii': 3})
        except Exception as ex:
            self.assertIsInstance(ex, mp.InputError)
            self.assertTrue(str(ex).find(
                'Cannot apply $inc to a value of non-numeric type.') >= 0)
        else:
            self.assertTrue(False)

        try:
            mp.op_inc({}, {'i': 'asdasd'})
        except Exception as ex:
            self.assertIsInstance(ex, mp.InputError)
            self.assertTrue(str(ex).find(
                'Cannot apply $inc to a value of non-numeric type.') >= 0)
        else:
            self.assertTrue(False)

        try:
            doc = {}
            mp.op_set(doc, {'i': 'asdasd'})
            mp.op_inc(doc, {'i': 2})
        except Exception as ex:
            self.assertIsInstance(ex, mp.InputError)
            self.assertTrue(str(ex).find(
                'Cannot apply $inc to a value of non-numeric type.') >= 0)
        else:
            self.assertTrue(False)

    def test_z_op_max(self):
        doc = {}
        mp.op_max(doc, {'h': 5})
        self.assertEqual(doc['h'], 5)
        mp.op_max(doc, {'h': 6})
        self.assertEqual(doc['h'], 6)
        mp.op_max(doc, {'h': 3})
        self.assertEqual(doc['h'], 6)

        mp.op_set(doc, {'ii': ['aa']})
        try:
            mp.op_max(doc, {'ii': 3})
        except Exception as ex:
            self.assertIsInstance(ex, mp.InputError)
            self.assertTrue(str(ex).find(
                'Cannot apply $max to a value of invaild type.') >= 0)
        else:
            self.assertTrue(False)

        try:
            mp.op_max({}, {'ii': []})
        except Exception as ex:
            self.assertIsInstance(ex, mp.InputError)
            self.assertTrue(str(ex).find(
                'Cannot apply $max to a value of invaild type.') >= 0)
        else:
            self.assertTrue(False)

    def test_z_op_min(self):
        doc = {}
        mp.op_min(doc, {'j': 5})
        self.assertEqual(doc['j'], 5)
        mp.op_min(doc, {'j': 6})
        self.assertEqual(doc['j'], 5)
        mp.op_min(doc, {'j': 3})
        self.assertEqual(doc['j'], 3)

        mp.op_set(doc, {'ii': ['aa']})
        try:
            mp.op_min(doc, {'ii': 3})
        except Exception as ex:
            self.assertIsInstance(ex, mp.InputError)
            self.assertTrue(str(ex).find(
                'Cannot apply $min to a value of invaild type.') >= 0)
        else:
            self.assertTrue(False)

        try:
            mp.op_min({}, {'ii': []})
        except Exception as ex:
            self.assertIsInstance(ex, mp.InputError)
            self.assertTrue(str(ex).find(
                'Cannot apply $min to a value of invaild type.') >= 0)
        else:
            self.assertTrue(False)

    def test_z_op_unset(self):
        try:
            mp.op_unset({}, {'j': ''})
        except Exception as ex:
            self.assertIsInstance(ex, mp.InputError)
            self.assertTrue(str(ex).find(
                'Cannot found field') >= 0)
        else:
            self.assertTrue(False)

        doc = {'j': 122}
        mp.op_unset(doc, {'j': ''})
        self.assertNotIn('j', doc)

    def test_z_op_rename(self):
        try:
            mp.op_rename({}, {'h': 'j'})
        except Exception as ex:
            self.assertIsInstance(ex, mp.InputError)
            self.assertTrue(str(ex).find(
                'Cannot found field') >= 0)
        else:
            self.assertTrue(False)

        doc = {'h': 6}
        mp.op_rename(doc, {'h': 'j'})
        self.assertIn('j', doc)
        self.assertNotIn('h', doc)
        self.assertEqual(doc['j'], 6)

    def test_z_op_muilt(self):
        doc = {}
        mp.op_set(doc, {'c.c': 5})
        self.assertEqual(doc['c']['c'], 5)
        try:
            mp.op_set(doc, {'c.c.c': 5})
        except Exception as ex:
            self.assertIsInstance(ex, mp.InputError)
            self.assertTrue(str(ex).find('Cannot create field') >= 0)
        else:
            self.assertTrue(False)

        mp.op_set(doc, {'a.a': 5})
        self.assertEqual(doc['a']['a'], 5)
        mp.op_set(doc, {'a.a': 6})
        self.assertEqual(doc['a']['a'], 6)
        mp.op_set(doc, {'a.a': 3})
        self.assertEqual(doc['a']['a'], 3)
        mp.op_set(doc, {'a.b.a': 5})
        self.assertEqual(doc['a']['b']['a'], 5)
        mp.op_set(doc, {'a.b.a': 6})
        self.assertEqual(doc['a']['b']['a'], 6)
        mp.op_set(doc, {'a.b.a': 3})
        self.assertEqual(doc['a']['b']['a'], 3)

        mp.op_inc(doc, {'i.i': 1})
        self.assertEqual(doc['i']['i'], 1)
        mp.op_inc(doc, {'i.i': 1})
        self.assertEqual(doc['i']['i'], 2)
        mp.op_inc(doc, {'i.i': 3})
        self.assertEqual(doc['i']['i'], 5)
        mp.op_inc(doc, {'i.b.i': 1})
        self.assertEqual(doc['i']['b']['i'], 1)
        mp.op_inc(doc, {'i.b.i': 1})
        self.assertEqual(doc['i']['b']['i'], 2)
        mp.op_inc(doc, {'i.b.i': 3})
        self.assertEqual(doc['i']['b']['i'], 5)
        try:
            mp.op_inc(doc, {'a.b.a': "asdsad"})
        except Exception as ex:
            self.assertIsInstance(ex, mp.InputError)
            self.assertTrue(str(ex).find(
                'Cannot apply $inc to a value of non-numeric type.') >= 0)
        else:
            self.assertTrue(False)

        mp.op_max(doc, {'h.h': 5})
        self.assertEqual(doc['h']['h'], 5)
        mp.op_max(doc, {'h.h': 6})
        self.assertEqual(doc['h']['h'], 6)
        mp.op_max(doc, {'h.h': 3})
        self.assertEqual(doc['h']['h'], 6)
        mp.op_max(doc, {'h.b.h': 5})
        self.assertEqual(doc['h']['b']['h'], 5)
        mp.op_max(doc, {'h.b.h': 6})
        self.assertEqual(doc['h']['b']['h'], 6)
        mp.op_max(doc, {'h.b.h': 3})
        self.assertEqual(doc['h']['b']['h'], 6)

        mp.op_min(doc, {'j.j': 5})
        self.assertEqual(doc['j']['j'], 5)
        mp.op_min(doc, {'j.j': 6})
        self.assertEqual(doc['j']['j'], 5)
        mp.op_min(doc, {'j.j': 3})
        self.assertEqual(doc['j']['j'], 3)
        mp.op_min(doc, {'j.b.j': 5})
        self.assertEqual(doc['j']['b']['j'], 5)
        mp.op_min(doc, {'j.b.j': 6})
        self.assertEqual(doc['j']['b']['j'], 5)
        mp.op_min(doc, {'j.b.j': 3})
        self.assertEqual(doc['j']['b']['j'], 3)

        mp.op_unset(doc, {'j.b.j': ''})
        self.assertNotIn('j', doc['j']['b'])
        mp.op_unset(doc, {'j': ''})
        self.assertNotIn('j', doc)

        mp.op_set(doc, {'d.d.d': 6})
        mp.op_rename(doc, {'d': 'j'})
        self.assertIn('j', doc)
        self.assertNotIn('d', doc)
        self.assertEqual(doc['j']['d']['d'], 6)
        mp.op_rename(doc, {'j': 'd'})
        self.assertEqual(doc['d']['d']['d'], 6)
        mp.op_rename(doc, {'d.d': 'j.j'})
        self.assertEqual(len(doc['d']), 0)
        self.assertEqual(doc['j']['j']['d'], 6)
        mp.op_rename(doc, {'j.j': 'd.d'})
        self.assertEqual(doc['d']['d']['d'], 6)
        mp.op_rename(doc, {'d.d': 'j'})
        self.assertEqual(doc['j']['d'], 6)

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

        mp.op_addtoset(doc, {'s.h': {'$each': [100, 101, 102]}})
        self.assertEqual(len(doc['s']['h']), 3)
        self.assertTrue(100 in doc['s']['h'])
        self.assertTrue(101 in doc['s']['h'])
        self.assertTrue(102 in doc['s']['h'])

        try:
            mp.op_addtoset({}, {'s.h': {'$each': ''}})
        except Exception as ex:
            self.assertIsInstance(ex, mp.InputError)
            self.assertTrue(str(ex).find(
                '$each invaild') >= 0)
        else:
            self.assertTrue(False)

        try:
            doc = {}
            mp.op_set(doc, {'s.h': 6})
            mp.op_addtoset(doc, {'s.h': 323})
        except Exception as ex:
            self.assertIsInstance(ex, mp.InputError)
            self.assertTrue(str(ex).find(
                'Cannot apply $addToSet to a value of non-array type.') >= 0)
        else:
            self.assertTrue(False)

        try:
            doc = {}
            mp.op_set(doc, {'s.h': 'sdsdsd'})
            mp.op_addtoset(doc, {'s.h': 323})
        except Exception as ex:
            self.assertIsInstance(ex, mp.InputError)
            self.assertTrue(str(ex).find(
                'Cannot apply $addToSet to a value of non-array type.') >= 0)
        else:
            self.assertTrue(False)

        try:
            doc = {}
            mp.op_set(doc, {'s.h': 'sdsdsd'})
            mp.op_addtoset(doc, {'s.h': {'$each': [234234]}})
        except Exception as ex:
            self.assertIsInstance(ex, mp.InputError)
            self.assertTrue(str(ex).find(
                'Cannot apply $addToSet to a value of non-array type.') >= 0)
        else:
            self.assertTrue(False)

    def test_z_op_datetime(self):
        doc = {}
        mp.op_datetime(doc, {'a': {}})
        datetime.datetime.strptime(doc['a'], '%Y-%m-%d %H:%M:%S.%f')
        mp.op_datetime(doc, {'b': {'$type': 'date'}})
        datetime.datetime.strptime(doc['b'], '%Y-%m-%d')
        mp.op_datetime(doc, {'c': {'$type': 'timestamp'}})
        datetime.datetime.strptime(doc['c'], '%Y-%m-%d %H:%M:%S.%f')

        doc = {}
        mp.op_datetime(doc, {'a.a': {}})
        datetime.datetime.strptime(doc['a']['a'], '%Y-%m-%d %H:%M:%S.%f')
        mp.op_datetime(doc, {'a.b': {'$type': 'date'}})
        datetime.datetime.strptime(doc['a']['b'], '%Y-%m-%d')
        mp.op_datetime(doc, {'b.b': {'$type': 'timestamp'}})
        datetime.datetime.strptime(doc['b']['b'], '%Y-%m-%d %H:%M:%S.%f')
        try:
            mp.op_datetime({}, {'ii': {'$type': 'timestam222'}})
        except Exception as ex:
            self.assertIsInstance(ex, mp.InputError)
            self.assertTrue(str(ex).find(
                'op_datetime $type invaild') >= 0)
        else:
            self.assertTrue(False)


if __name__ == '__main__':
    unittest.main()
