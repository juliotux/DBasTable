# Licensed under a 3-clause BSD style license - see LICENSE.rst
# flake8: noqa: F403, F405

from dbastable import SQLColumn, SQLRow, SQLTable, SQLDatabase, Where
import numpy as np
from astropy.table import Table
import tempfile
import sys
import unittest
import os

from dbastable.tests.mixins import TestCaseWithNumpyCompare


class TestSQLDatabaseCreationModify(TestCaseWithNumpyCompare):
    def test_sql_db_creation(self):
        db = SQLDatabase(':memory:')
        self.assertEqual(db.table_names, [])
        self.assertEqual(len(db), 0)

        db.add_table('test')
        self.assertEqual(db.table_names, ['test'])
        self.assertEqual(len(db), 1)

    def test_sql_add_column_name_and_data(self):
        db = SQLDatabase(':memory:')
        db.add_table('test')
        db.add_column('test', 'a', data=np.arange(10, 20))
        db.add_column('test', 'b', data=np.arange(20, 30))

        self.assertEqualArray(db.get_column('test', 'a').values,
                              np.arange(10, 20))
        self.assertEqualArray(db.get_column('test', 'b').values,
                              np.arange(20, 30))
        self.assertEqualArray(db.column_names('test'),
                              ['a', 'b'])
        self.assertEqual(len(db), 1)
        self.assertEqual(db.table_names, ['test'])

    def test_sql_add_column_only_name(self):
        db = SQLDatabase(':memory:')
        db.add_table('test')
        db.add_column('test', 'a')
        db.add_column('test', 'b')

        self.assertEqual(db.get_column('test', 'a').values, [])
        self.assertEqual(db.get_column('test', 'b').values, [])
        self.assertEqual(db.column_names('test'), ['a', 'b'])
        self.assertEqual(len(db), 1)
        self.assertEqual(db.table_names, ['test'])

    def test_sql_add_table_from_data_table(self):
        db = SQLDatabase(':memory:')
        d = Table(names=['a', 'b'], data=[
                  np.arange(10, 20), np.arange(20, 30)])
        db.add_table('test', data=d)

        self.assertEqualArray(db.get_column(
            'test', 'a').values, np.arange(10, 20))
        self.assertEqualArray(db.get_column(
            'test', 'b').values, np.arange(20, 30))
        self.assertEqualArray(db.column_names('test'), ['a', 'b'])
        self.assertEqual(len(db), 1)
        self.assertEqualArray(db.table_names, ['test'])

    def test_sql_add_table_from_data_ndarray(self):
        dtype = [('a', 'i4'), ('b', 'f8')]
        data = np.array([(1, 2.0), (3, 4.0), (5, 6.0), (7, 8.0)], dtype=dtype)
        db = SQLDatabase(':memory:')
        db.add_table('test', data=data)

        self.assertEqualArray(db.get_column('test', 'a').values,
                              [1, 3, 5, 7])
        self.assertEqualArray(db.get_column('test', 'b').values,
                              [2.0, 4.0, 6.0, 8.0])
        self.assertEqualArray(db.column_names('test'), ['a', 'b'])
        self.assertEqual(len(db), 1)
        self.assertEqualArray(db.table_names, ['test'])

    def test_sql_add_table_from_data_dict(self):
        d = {'a': np.arange(10, 20), 'b': np.arange(20, 30)}
        db = SQLDatabase(':memory:')
        db.add_table('test', data=d)

        self.assertEqualArray(db.get_column('test', 'a').values,
                              np.arange(10, 20))
        self.assertEqualArray(db.get_column('test', 'b').values,
                              np.arange(20, 30))
        self.assertEqualArray(db.column_names('test'),
                              ['a', 'b'])
        self.assertEqual(len(db), 1)
        self.assertEqualArray(db.table_names,
                              ['test'])

    def test_sql_add_table_from_data_ndarray_untyped(self):
        # Untyped ndarray should fail in get column names
        data = np.array([(1, 2.0), (3, 4.0), (5, 6.0), (7, 8.0)])
        db = SQLDatabase(':memory:')
        with self.assertRaises(ValueError):
            db.add_table('test', data=data)

    def test_sql_add_table_from_data_invalid(self):
        for data, error in [([1, 2, 3], ValueError),
                            (1, TypeError),
                            (1.0, TypeError),
                            ('test', TypeError)]:
            db = SQLDatabase(':memory:')
            with self.assertRaises(error):
                db.add_table('test', data=data)

    def test_sql_add_table_columns(self):
        db = SQLDatabase(':memory:')
        db.add_table('test', columns=['a', 'b'])

        self.assertEqual(db.get_column('test', 'a').values, [])
        self.assertEqual(db.get_column('test', 'b').values, [])
        self.assertEqual(db.column_names('test'), ['a', 'b'])
        self.assertEqual(len(db), 1)
        self.assertEqual(db.table_names, ['test'])

    def test_sql_add_table_columns_data(self):
        db = SQLDatabase(':memory:')
        with self.assertRaises(ValueError):
            db.add_table('test', columns=['a', 'b'], data=[1, 2, 3])

    def test_sql_add_row(self):
        db = SQLDatabase(':memory:')
        db.add_table('test')
        db.add_column('test', 'a')
        db.add_column('test', 'b')
        db.add_rows('test', dict(a=1, b=2))
        db.add_rows('test', dict(a=[3, 5], b=[4, 6]))

        self.assertEqual(db.get_column('test', 'a').values, [1, 3, 5])
        self.assertEqual(db.get_column('test', 'b').values, [2, 4, 6])
        self.assertEqual(len(db), 1)
        self.assertEqual(db.table_names, ['test'])

    def test_sql_add_row_types(self):
        db = SQLDatabase(':memory:')
        db.add_table('test')
        for k in ['a', 'b', 'c', 'd', 'e']:
            db.add_column('test', k)

        db.add_rows('test', dict(a=1, b='a', c=True, d=b'a', e=3.14))
        db.add_rows('test', dict(a=2, b='b', c=False, d=b'b', e=2.71))

        self.assertEqual(db.get_column('test', 'a').values, [1, 2])
        self.assertEqual(db.get_column('test', 'b').values, ['a', 'b'])
        self.assertEqual(db.get_column('test', 'c').values, [1, 0])
        self.assertEqual(db.get_column('test', 'd').values, [b'a', b'b'])
        self.assertAlmostEqualArray(db.get_column('test', 'e').values,
                                    [3.14, 2.71])
        self.assertEqual(len(db), 1)
        self.assertEqual(db.table_names, ['test'])

    def test_sql_add_row_invalid(self):
        db = SQLDatabase(':memory:')
        db.add_table('test')
        db.add_column('test', 'a')
        db.add_column('test', 'b')
        with self.assertRaises(ValueError):
            db.add_rows('test', [1, 2, 3])

    def test_sql_add_row_add_columns(self):
        db = SQLDatabase(':memory:')
        db.add_table('test')
        db.add_column('test', 'a')
        db.add_column('test', 'b')
        db.add_rows('test', dict(a=1, b=2))
        with self.assertRaises(KeyError):
            # add a row with non-existing columns must raise error
            db.add_rows('test', dict(a=3, c=4), add_columns=False)
        db.add_rows('test', dict(a=5, d=6), add_columns=True)

        self.assertEqual(db.get_column('test', 'a').values, [1, 5])
        self.assertEqual(db.get_column('test', 'b').values, [2, None])
        self.assertEqual(len(db), 1)
        self.assertEqual(db.table_names, ['test'])
        self.assertEqual(db.column_names('test'), ['a', 'b', 'd'])

    def test_sql_add_row_superpass_64_limit(self):
        db = SQLDatabase(':memory:')
        db.add_table('test')
        db.add_rows('test', {f'col{i}': np.arange(10) for i in range(128)},
                    add_columns=True)
        self.assertEqualArray(db.column_names('test'),
                              [f'col{i}' for i in range(128)])

        for i, v in enumerate(db.select('test')):
            self.assertEqualArray(v, [i]*128)

    def test_sqltable_add_column(self):
        db = SQLDatabase(':memory:')
        db.add_table('test')
        db['test'].add_column('a')
        db['test'].add_column('b')
        db['test'].add_column('c', data=[1, 2, 3])

        self.assertEqual(db.get_column('test', 'a').values, [None, None, None])
        self.assertEqual(db.get_column('test', 'b').values, [None, None, None])
        self.assertEqual(db.get_column('test', 'c').values, [1, 2, 3])
        self.assertEqual(len(db), 1)
        self.assertEqual(db.table_names, ['test'])
        self.assertEqual(db.column_names('test'), ['a', 'b', 'c'])
        self.assertEqual(len(db['test']), 3)

    def test_sqltable_add_row_add_columns(self):
        db = SQLDatabase(':memory:')
        db.add_table('test')
        db.add_column('test', 'a')
        db.add_column('test', 'b')
        db['test'].add_rows(dict(a=1, b=2))
        with self.assertRaises(KeyError):
            # add a row with non-existing columns must raise error
            db['test'].add_rows(dict(a=3, c=4), add_columns=False)
        db['test'].add_rows(dict(a=5, d=6), add_columns=True)

        self.assertEqual(db.get_column('test', 'a').values, [1, 5])
        self.assertEqual(db.get_column('test', 'b').values, [2, None])
        self.assertEqual(len(db), 1)
        self.assertEqual(db.table_names, ['test'])
        self.assertEqual(db.column_names('test'), ['a', 'b', 'd'])

    def test_sql_set_column(self):
        db = SQLDatabase(':memory:')
        db.add_table('test')
        db.add_column('test', 'a', [1, 3, 5])
        db.add_column('test', 'b', [2, 4, 6])

        db.set_column('test', 'a', [10, 20, 30])
        db.set_column('test', 'b', [20, 40, 60])

        self.assertEqual(db.get_column('test', 'a').values, [10, 20, 30])
        self.assertEqual(db.get_column('test', 'b').values, [20, 40, 60])

        with self.assertRaises(KeyError):
            db.set_column('test', 'c', [10, 20, 30])
        with self.assertRaises(ValueError):
            db.set_column('test', 'a', [10, 20, 30, 40])

    def test_sql_set_row(self):
        db = SQLDatabase(':memory:')
        db.add_table('test')
        db.add_column('test', 'a', [1, 3, 5])
        db.add_column('test', 'b', [2, 4, 6])

        db.set_row('test', 0, {'a': 10, 'b': 20})
        db.set_row('test', 1, [20, 40])
        db.set_row('test', 2, np.array([30, 60]))

        self.assertEqual(db.get_column('test', 'a').values, [10, 20, 30])
        self.assertEqual(db.get_column('test', 'b').values, [20, 40, 60])

        with self.assertRaises(IndexError):
            db.set_row('test', 3, {'a': 10, 'b': 20})
        with self.assertRaises(IndexError):
            db.set_row('test', -4, {'a': 10, 'b': 20})

    def test_sql_set_item(self):
        db = SQLDatabase(':memory:')
        db.add_table('test')
        db.add_column('test', 'a', [1, 3, 5])
        db.add_column('test', 'b', [2, 4, 6])

        db.set_item('test', 'a', 0, 10)
        db.set_item('test', 'b', 1, 'a')
        self.assertEqual(db.get_column('test', 'a').values, [10, 3, 5])
        self.assertEqual(db.get_column('test', 'b').values, [2, 'a', 6])

    def test_sql_setitem_tuple_only(self):
        db = SQLDatabase(':memory:')
        db.add_table('test')
        db.add_column('test', 'a', data=np.arange(10, 20))
        db.add_column('test', 'b', data=np.arange(20, 30))

        with self.assertRaises(KeyError):
            db[1] = 0
        with self.assertRaises(KeyError):
            db['notable'] = 0
        with self.assertRaises(KeyError):
            db[['test', 0]] = 0
        with self.assertRaises(KeyError):
            db[1, 0] = 0

    def test_sql_setitem(self):
        db = SQLDatabase(':memory:')
        db.add_table('test')
        db.add_column('test', 'a', data=np.arange(10, 20))
        db.add_column('test', 'b', data=np.arange(20, 30))

        db['test', 'a'] = np.arange(50, 60)
        db['test', 0] = {'a': 1, 'b': 2}
        db['test', 'b', 5] = -999

        expect = np.transpose([np.arange(50, 60), np.arange(20, 30)])
        expect[0] = [1, 2]
        expect[5, 1] = -999

        self.assertEqualArray(db['test'].values, expect)

    def test_sql_droptable(self):
        db = SQLDatabase(':memory:')
        db.add_table('test')
        db.add_column('test', 'a', [1, 3, 5])
        db.add_column('test', 'b', [2, 4, 6])

        db.drop_table('test')
        self.assertEqual(db.table_names, [])
        with self.assertRaises(KeyError):
            db['test']

    def test_sql_copy(self):
        db = SQLDatabase(':memory:')
        db.add_table('test')
        db.add_column('test', 'a', [1, 3, 5])
        db.add_column('test', 'b', [2, 4, 6])

        db2 = db.copy()
        self.assertEqual(db2.table_names, ['test'])
        self.assertEqual(db2.column_names('test'), ['a', 'b'])
        self.assertEqual(db2.get_column('test', 'a').values, [1, 3, 5])
        self.assertEqual(db2.get_column('test', 'b').values, [2, 4, 6])

    def test_sql_copy_indexes(self):
        arr_a = np.arange(1, 101, 2)
        arr_b = np.arange(2, 102, 2)[::-1]
        db = SQLDatabase(':memory:')
        db.add_table('test')
        db.add_column('test', 'a', arr_a)
        db.add_column('test', 'b', arr_b)

        indx = [30, 24, 32, 11]
        db2 = db.copy(indexes={'test': indx})
        self.assertEqual(db2.table_names, ['test'])
        self.assertEqual(db2.column_names('test'), ['a', 'b'])
        self.assertEqualArray(db2.get_column('test', 'a').values,
                              arr_a[indx])
        self.assertEqualArray(db2.get_column('test', 'b').values,
                              arr_b[indx])

    def test_sql_delete_row(self):
        db = SQLDatabase(':memory:')
        db.add_table('test')
        db.add_column('test', 'a', [1, 3, 5])
        db.add_column('test', 'b', [2, 4, 6])

        db.delete_row('test', 1)
        self.assertEqualArray(db.get_column('test', 'a').values, [1, 5])
        self.assertEqualArray(db.get_column('test', 'b').values, [2, 6])

        with self.assertRaises(IndexError):
            db.delete_row('test', 2)
        with self.assertRaises(IndexError):
            db.delete_row('test', -4)

    def test_sql_delete_column(self):
        db = SQLDatabase(':memory:')
        db.add_table('test')
        db.add_column('test', 'a', [1, 3, 5])
        db.add_column('test', 'b', [2, 4, 6])

        db.delete_column('test', 'b')
        self.assertEqualArray(db.column_names('test'), ['a'])
        self.assertEqualArray(db.get_column('test', 'a').values,
                              [1, 3, 5])

        with self.assertRaisesRegex(KeyError, 'does not exist'):
            db.delete_column('test', 'b')
        with self.assertRaisesRegex(ValueError, 'protected name'):
            db.delete_column('test', 'table')


class TestSQLDatabaseAccess(TestCaseWithNumpyCompare):
    def test_sql_get_table(self):
        db = SQLDatabase(':memory:')
        db.add_table('test')
        db.add_column('test', 'a', data=np.arange(10, 20))
        db.add_column('test', 'b', data=np.arange(20, 30))

        self.assertEqualArray(db.get_table('test').values,
                              list(zip(np.arange(10, 20),
                                       np.arange(20, 30))))
        self.assertIsInstance(db.get_table('test'), SQLTable)

        with self.assertRaises(KeyError):
            db.get_table('not_a_table')

    def test_sql_get_table_empty(self):
        db = SQLDatabase(':memory:')
        db.add_table('test')

        self.assertEqual(len(db.get_table('test')), 0)
        self.assertIsInstance(db.get_table('test'), SQLTable)

    def test_sql_get_column(self):
        db = SQLDatabase(':memory:')
        db.add_table('test')
        db.add_column('test', 'a', data=np.arange(10, 20))
        db.add_column('test', 'b', data=np.arange(20, 30))

        self.assertEqualArray(db.get_column('test', 'a').values,
                              np.arange(10, 20))
        self.assertEqualArray(db.get_column('test', 'b').values,
                              np.arange(20, 30))
        self.assertIsInstance(db.get_column('test', 'a'), SQLColumn)
        self.assertIsInstance(db.get_column('test', 'b'), SQLColumn)

        # same access from table
        self.assertEqualArray(db.get_table('test').get_column('a').values,
                              np.arange(10, 20))
        self.assertEqualArray(db.get_table('test').get_column('b').values,
                              np.arange(20, 30))
        self.assertIsInstance(db.get_table('test').get_column('a'), SQLColumn)
        self.assertIsInstance(db.get_table('test').get_column('b'), SQLColumn)

        with self.assertRaises(KeyError):
            db.get_column('test', 'c')
        with self.assertRaises(KeyError):
            db.get_table('test').get_column('c')

    def test_sql_get_row(self):
        db = SQLDatabase(':memory:')
        db.add_table('test')
        db.add_column('test', 'a', data=np.arange(10, 20))
        db.add_column('test', 'b', data=np.arange(20, 30))

        self.assertEqualArray(db.get_row('test', 4).values, (14, 24))
        self.assertIsInstance(db.get_row('test', 4), SQLRow)

        self.assertEqualArray(db.get_row('test', -1).values, (19, 29))
        self.assertIsInstance(db.get_row('test', -1), SQLRow)

        # same access from table
        self.assertEqualArray(db.get_table('test').get_row(4).values, [14, 24])
        self.assertIsInstance(db.get_table('test').get_row(4), SQLRow)

        with self.assertRaises(IndexError):
            db.get_row('test', 11)
        with self.assertRaises(IndexError):
            db.get_row('test', -11)
        with self.assertRaises(IndexError):
            db.get_table('test').get_row(11)
        with self.assertRaises(IndexError):
            db.get_table('test').get_row(-11)

    def test_sql_getitem(self):
        db = SQLDatabase(':memory:')
        db.add_table('test')
        db.add_column('test', 'a', data=np.arange(10, 20))
        db.add_column('test', 'b', data=np.arange(20, 30))

        self.assertEqualArray(db['test']['a'].values, np.arange(10, 20))
        self.assertEqualArray(db['test']['b'].values, np.arange(20, 30))
        self.assertIsInstance(db['test']['a'], SQLColumn)
        self.assertIsInstance(db['test']['b'], SQLColumn)

        self.assertEqualArray(db['test'][4].values, (14, 24))
        self.assertIsInstance(db['test'][4], SQLRow)
        self.assertEqualArray(db['test'][-1].values, (19, 29))
        self.assertIsInstance(db['test'][-1], SQLRow)

        with self.assertRaises(KeyError):
            db['test']['c']
        with self.assertRaises(KeyError):
            db['not_a_table']['a']

        with self.assertRaises(IndexError):
            db['test'][11]
        with self.assertRaises(IndexError):
            db['test'][-11]

    def test_sql_getitem_tuple(self):
        db = SQLDatabase(':memory:')
        db.add_table('test')
        db.add_column('test', 'a', data=np.arange(10, 20))
        db.add_column('test', 'b', data=np.arange(20, 30))

        self.assertEqualArray(db['test', 'a'].values, np.arange(10, 20))
        self.assertEqualArray(db['test', 'b'].values, np.arange(20, 30))
        self.assertIsInstance(db['test', 'a'], SQLColumn)
        self.assertIsInstance(db['test', 'b'], SQLColumn)

        self.assertEqualArray(db['test', 4].values, (14, 24))
        self.assertIsInstance(db['test', 4], SQLRow)
        self.assertEqualArray(db['test', -1].values, (19, 29))
        self.assertIsInstance(db['test', -1], SQLRow)

        self.assertEqual(db['test', 'a', 4], 14)
        self.assertEqual(db['test', 'b', 4], 24)
        self.assertEqual(db['test', 'a', -1], 19)
        self.assertEqual(db['test', 'b', -1], 29)
        self.assertEqual(db['test', 4, 'a'], 14)
        self.assertEqual(db['test', 4, 'b'], 24)
        self.assertEqual(db['test', -1, 'a'], 19)
        self.assertEqual(db['test', -1, 'b'], 29)

    def test_sql_getitem_table_force(self):
        db = SQLDatabase(':memory:')
        db.add_table('test')
        db.add_column('test', 'a', data=np.arange(10, 20))
        db.add_column('test', 'b', data=np.arange(20, 30))

        with self.assertRaises(ValueError):
            db[1]
        with self.assertRaises(ValueError):
            db[1, 2]
        with self.assertRaises(ValueError):
            db[1, 2, 'test']
        with self.assertRaises(ValueError):
            db[[1, 2], 'test']


class TestSQLDatabasePropsComms(TestCaseWithNumpyCompare):
    def test_sql_select_where(self):
        db = SQLDatabase(':memory:')
        db.add_table('test')
        db.add_column('test', 'a', data=np.arange(10, 20))
        db.add_column('test', 'b', data=np.arange(20, 30))

        a = db.select('test', columns='a', where={'a': 15})
        self.assertEqualArray(a, [(15,)])

        a = db.select('test', columns=['a', 'b'], where={'b': 22})
        self.assertEqualArray(a, [(12, 22)])

        a = db.select('test', columns=['a', 'b'], where=None)
        self.assertEqualArray(
            a, list(zip(np.arange(10, 20), np.arange(20, 30))))

        a = db.select('test', columns=['a', 'b'],
                      where=[Where('a', '>', 12),
                             Where('b', '<', 26)])
        self.assertEqualArray(a, [(13, 23), (14, 24), (15, 25)])

    def test_sql_select_limit_offset(self):
        db = SQLDatabase(':memory:')
        db.add_table('test')
        db.add_column('test', 'a', data=np.arange(10, 20))
        db.add_column('test', 'b', data=np.arange(20, 30))

        a = db.select('test', columns='a', limit=1)
        self.assertEqualArray(a, [(10, )])

        a = db.select('test', columns='a', limit=3, offset=2)
        self.assertEqualArray(a, [[12], [13], [14]])

    def test_sql_select_invalid(self):
        db = SQLDatabase(':memory:')
        db.add_table('test')
        db.add_column('test', 'a', data=np.arange(10, 20))
        db.add_column('test', 'b', data=np.arange(20, 30))

        with self.assertRaisesRegex(KeyError,
                                    'not found'):
            db.select('test', columns=['c'])

        with self.assertRaisesRegex(ValueError,
                                    'offset cannot be used without limit.'):
            db.select('test', columns='a', offset=1)

        with self.assertRaises(TypeError):
            db.select('test', columns='a', where=1)

        with self.assertRaisesRegex(TypeError, 'if where is a list'):
            db.select('test', columns='a', where=[1, 2, 3])

        with self.assertRaises(TypeError):
            db.select('test', limit=3.14)

        with self.assertRaises(TypeError):
            db.select('test', order=5)

    def test_sql_select_order(self):
        db = SQLDatabase(':memory:')
        db.add_table('test')
        db.add_column('test', 'a', data=np.arange(10, 20))
        db.add_column('test', 'b', data=np.arange(20, 30)[::-1])

        a = db.select('test', order='b')
        self.assertEqual(a, list(zip(np.arange(10, 20),
                                 np.arange(20, 30)[::-1]))[::-1])

        a = db.select('test', order='b', limit=2)
        self.assertEqualArray(a, [(19, 20), (18, 21)])

        a = db.select('test', order='b', limit=2, offset=2)
        self.assertEqualArray(a, [(17, 22), (16, 23)])

        a = db.select('test', order='b', where=Where('a', '<', 15))
        self.assertEqualArray(a, [(14, 25), (13, 26), (12, 27), (11, 28), (10, 29)])

        a = db.select('test', order='b', where=Where('a', '<', 15),
                      limit=3)
        self.assertEqualArray(a, [(14, 25), (13, 26), (12, 27)])

        a = db.select('test', order='b', where=Where('a', '<', 15),
                      limit=3, offset=2)
        self.assertEqualArray(a, [(12, 27), (11, 28), (10, 29)])

    def test_sql_count(self):
        db = SQLDatabase(':memory:')
        db.add_table('test')
        db.add_column('test', 'a', data=np.arange(10, 20))
        db.add_column('test', 'b', data=np.arange(20, 30))

        self.assertEqual(db.count('test'), 10)
        self.assertEqual(db.count('test', where={'a': 15}), 1)
        self.assertEqual(db.count('test', where={'a': 15, 'b': 22}), 0)
        self.assertEqual(db.count('test', where=Where('a', '>', 15)), 4)
        self.assertEqual(db.count('test', where=[Where('a', '>', 15),
                                                 Where('b', '<', 27)]), 1)

    @unittest.skipIf(sys.platform.startswith("win"),
                     "problems with temp_path")
    def test_sql_prop_db(self):
        db = SQLDatabase(':memory:')
        self.assertEqual(db.db, ':memory:')

        tmp_path = tempfile.mkdtemp()
        path = os.path.join(tmp_path, 'test.db')
        db = SQLDatabase(path)
        self.assertEqual(db.db, path)
        os.remove(path)
        os.removedirs(tmp_path)

    def test_sql_prop_table_names(self):
        db = SQLDatabase(':memory:')
        db.add_table('test')
        db.add_table('test2')
        self.assertEqual(db.table_names, ['test', 'test2'])

    def test_sql_prop_column_names(self):
        db = SQLDatabase(':memory:')
        db.add_table('test')
        db.add_column('test', 'a', data=np.arange(10, 20))
        db.add_column('test', 'b', data=np.arange(20, 30))
        self.assertEqual(db.column_names('test'), ['a', 'b'])

    def test_sql_repr(self):
        db = SQLDatabase(':memory:')
        db.add_table('test')
        db.add_column('test', 'a', data=np.arange(10, 20))
        db.add_column('test', 'b', data=np.arange(20, 30))
        db.add_table('test2')
        db.add_column('test2', 'a', data=np.arange(10, 20))
        db.add_column('test2', 'b', data=np.arange(20, 30))

        expect = f"SQLDatabase ':memory:' at {hex(id(db))}:\n"
        expect += "\ttest: 2 columns 10 rows\n"
        expect += "\ttest2: 2 columns 10 rows"
        self.assertEqual(repr(db), expect)

        db = SQLDatabase(':memory:')
        expect = f"SQLDatabase ':memory:' at {hex(id(db))}:\n"
        expect += "\tEmpty database."
        self.assertEqual(repr(db), expect)

    def test_sql_len(self):
        db = SQLDatabase(':memory:')
        db.add_table('test')
        db.add_table('test2')
        db.add_table('test3')

        self.assertEqual(len(db), 3)

    def test_sql_index_of(self):
        db = SQLDatabase(':memory:')
        db.add_table('test')
        db.add_column('test', 'a', data=np.arange(10, 20))
        db.add_column('test', 'b', data=np.arange(20, 30))

        self.assertEqual(db.index_of('test', {'a': 15}), 5)
        self.assertEqual(db.index_of('test', Where('b', '>=', 27)),
                         [7, 8, 9])
        self.assertEqual(db.index_of('test', {'a': 1, 'b': 2}), [])
