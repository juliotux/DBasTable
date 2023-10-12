# Licensed under a 3-clause BSD style license - see LICENSE.rst
# flake8: noqa: F403, F405

import unittest
from dbastable import SQLColumn, SQLRow, SQLTable, SQLDatabase
from dbastable._parse_tools import _sanitize_colnames
import numpy as np
from astropy.table import Table
import sqlite3


class TestCaseWithNumpyCompare(unittest.TestCase):
    def assertEqualArray(self, *args):
        return np.testing.assert_array_equal(*args)


class TestSanitizeColnames(TestCaseWithNumpyCompare):
    def test_sanitize_string(self):
        for i in ['test-2', 'test!2', 'test@2', 'test#2', 'test$2',
                  'test&2', 'test*2', 'test(2)', 'test)2', 'test[2]', 'test]2',
                  'test{2}', 'test}2', 'test|2', 'test\\2', 'test^2', 'test~2'
                  'test"2', 'test\'2', 'test`2', 'test<2', 'test>2', 'test=2',
                  'test,2', 'test;2', 'test:2', 'test?2', 'test/2']:
            with self.assertRaises(ValueError):
                _sanitize_colnames(i)

        for i in ['test', 'test_1', 'test_1_2', 'test_1_2', 'Test', 'Test_1']:
            self.assertEqual(_sanitize_colnames(i), i.lower())


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
        d = Table(names=['a', 'b'], data=[np.arange(10, 20), np.arange(20, 30)])
        db.add_table('test', data=d)

        self.assertEqualArray(db.get_column('test', 'a').values, np.arange(10, 20))
        self.assertEqualArray(db.get_column('test', 'b').values, np.arange(20, 30))
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
        self.assertAlmostEquals(db.get_column('test', 'e').values, [3.14, 2.71])
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
        db.add_rows('test', dict(a=3, c=4), add_columns=False)
        db.add_rows('test', dict(a=5, d=6), add_columns=True)

        self.assertEqual(db.get_column('test', 'a').values, [1, 3, 5])
        self.assertEqual(db.get_column('test', 'b').values, [2, None, None])
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
        db['test'].add_rows(dict(a=3, c=4), add_columns=False)
        db['test'].add_rows(dict(a=5, d=6), add_columns=True)

        self.assertEqual(db.get_column('test', 'a').values, [1, 3, 5])
        self.assertEqual(db.get_column('test', 'b').values, [2, None, None])
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

        db.set_row('test', 0, dict(a=10, b=20))
        db.set_row('test', 1, [20, 40])
        db.set_row('test', 2, np.array([30, 60]))

        self.assertEqual(db.get_column('test', 'a').values, [10, 20, 30])
        self.assertEqual(db.get_column('test', 'b').values, [20, 40, 60])

        with self.assertRaises(IndexError):
            db.set_row('test', 3, dict(a=10, b=20))
        with self.assertRaises(IndexError):
            db.set_row('test', -4, dict(a=10, b=20))

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
        db = SQLDatabase(':memory:')
        db.add_table('test')
        db.add_column('test', 'a', np.arange(1, 101, 2))
        db.add_column('test', 'b', np.arange(2, 102, 2))

        db2 = db.copy(indexes={'test': [30, 24, 32, 11]})
        self.assertEqual(db2.table_names, ['test'])
        self.assertEqual(db2.column_names('test'), ['a', 'b'])
        self.assertEqual(db2.get_column('test', 'a').values, [23, 49, 61, 65])
        self.assertEqual(db2.get_column('test', 'b').values, [24, 50, 62, 66])

    def test_sql_delete_row(self):
        db = SQLDatabase(':memory:')
        db.add_table('test')
        db.add_column('test', 'a', [1, 3, 5])
        db.add_column('test', 'b', [2, 4, 6])

        db.delete_row('test', 1)
        self.assertEqual(db.get_column('test', 'a').values, [1, 5])
        self.assertEqual(db.get_column('test', 'b').values, [2, 6])

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

        self.assertEqual(db.get_table('test').values, list(zip(np.arange(10, 20),
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
        self.assertEqual(a, [15])

        a = db.select('test', columns=['a', 'b'], where={'b': 22})
        self.assertEqualArray(a, [(12, 22)])

        a = db.select('test', columns=['a', 'b'], where=None)
        self.assertEqualArray(a, list(zip(np.arange(10, 20), np.arange(20, 30))))

        a = db.select('test', columns=['a', 'b'], where=['a > 12', 'b < 26'])
        self.assertEqualArray(a, [(13, 23), (14, 24), (15, 25)])

    def test_sql_select_limit_offset(self):
        db = SQLDatabase(':memory:')
        db.add_table('test')
        db.add_column('test', 'a', data=np.arange(10, 20))
        db.add_column('test', 'b', data=np.arange(20, 30))

        a = db.select('test', columns='a', limit=1)
        self.assertEqual(a, 10)

        a = db.select('test', columns='a', limit=3, offset=2)
        self.assertEqual(a, [[12], [13], [14]])

    def test_sql_select_invalid(self):
        db = SQLDatabase(':memory:')
        db.add_table('test')
        db.add_column('test', 'a', data=np.arange(10, 20))
        db.add_column('test', 'b', data=np.arange(20, 30))

        with self.assertRaises(sqlite3.OperationalError,
                           match='no such column: c'):
            db.select('test', columns=['c'])

        with self.assertRaises(ValueError,
                           match='offset cannot be used without limit.'):
            db.select('test', columns='a', offset=1)

        with self.assertRaises(TypeError, match='where must be'):
            db.select('test', columns='a', where=1)

        with self.assertRaises(TypeError, match='if where is a list'):
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
        self.assertEqual(a, [(19, 20), (18, 21)])

        a = db.select('test', order='b', limit=2, offset=2)
        self.assertEqual(a, [(17, 22), (16, 23)])

        a = db.select('test', order='b', where='a < 15')
        self.assertEqual(a, [(14, 25), (13, 26), (12, 27), (11, 28), (10, 29)])


        a = db.select('test', order='b', where='a < 15', limit=3)
        self.assertEqual(a, [(14, 25), (13, 26), (12, 27)])

        a = db.select('test', order='b', where='a < 15', limit=3, offset=2)
        self.assertEqual(a, [(12, 27), (11, 28), (10, 29)])

    def test_sql_count(self):
        db = SQLDatabase(':memory:')
        db.add_table('test')
        db.add_column('test', 'a', data=np.arange(10, 20))
        db.add_column('test', 'b', data=np.arange(20, 30))

        self.assertEqual(db.count('test'), 10)
        self.assertEqual(db.count('test', where={'a': 15}), 1)
        self.assertEqual(db.count('test', where={'a': 15, 'b': 22}), 0)
        self.assertEqual(db.count('test', where='a > 15'), 4)
        self.assertEqual(db.count('test', where=['a > 15', 'b < 27']), 1)

    def test_sql_prop_db(self, tmp_path):
        db = SQLDatabase(':memory:')
        self.assertEqual(db.db, ':memory:')

        db = SQLDatabase(str(tmp_path / 'test.db'))
        self.assertEqual(db.db, str(tmp_path / 'test.db'))

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
        self.assertEqual(db.index_of('test', 'b >= 27'), [7, 8, 9])
        self.assertEqual(db.index_of('test', {'a': 1, 'b': 2}), [])


class TestSQLRow(TestCaseWithNumpyCompare):
    @property
    def db(self):
        db = SQLDatabase(':memory:')
        db.add_table('test')
        db.add_column('test', 'a', data=np.arange(10, 20))
        db.add_column('test', 'b', data=np.arange(20, 30))
        return db

    def test_row_basic_properties(self):
        db = self.db
        row = db['test'][0]
        self.assertIsInstance(row, SQLRow)
        self.assertEqual(row.table, 'test')
        self.assertEqual(row.index, 0)
        self.assertEqual(row.column_names, ['a', 'b'])
        self.assertEqual(row.keys, ['a', 'b'])
        self.assertEqual(row.values, [10, 20])
        self.assertEqual(row.as_dict(), {'a': 10, 'b': 20})

    def test_row_iter(self):
        db = self.db
        row = db['test'][0]
        self.assertIsInstance(row, SQLRow)

        v = 10
        for i in row:
            self.assertEqual(i, v)
            v += 10

    def test_row_getitem(self):
        db = self.db
        row = db['test'][0]
        self.assertIsInstance(row, SQLRow)

        self.assertEqual(row['a'], 10)
        self.assertEqual(row['b'], 20)

        with self.assertRaises(KeyError):
            row['c']

        self.assertEqual(row[0], 10)
        self.assertEqual(row[1], 20)
        self.assertEqual(row[-1], 20)
        self.assertEqual(row[-2], 10)

        with self.assertRaises(IndexError):
            row[2]
        with self.assertRaises(IndexError):
            row[-3]

    def test_row_setitem(self):
        db = self.db
        row = db['test'][0]
        self.assertIsInstance(row, SQLRow)

        row['a'] = 1
        row['b'] = 1
        self.assertEqual(db['test']['a'], [1, 11, 12, 13, 14,
                                       15, 16, 17, 18, 19])
        self.assertEqual(db['test']['b'], [1, 21, 22, 23, 24,
                                       25, 26, 27, 28, 29])

        with self.assertRaises(KeyError):
            row['c'] = 1
        with self.assertRaises(KeyError):
            row[2] = 1
        with self.assertRaises(KeyError):
            row[-3] = 1

    def test_row_contains(self):
        db = self.db
        row = db['test'][0]
        self.assertIsInstance(row, SQLRow)

        self.assertTrue(10 in row)
        self.assertTrue(20 in row)
        self.assertFalse('c' in row)
        self.assertFalse('a' in row)
        self.assertFalse('b' in row)

    def test_row_repr(self):
        db = self.db
        row = db['test'][0]
        self.assertIsInstance(row, SQLRow)
        self.assertEqual(repr(row), "SQLRow 0 in table 'test' {'a': 10, 'b': 20}")


class TestSQLTable(TestCaseWithNumpyCompare):
    @property
    def db(self):
        db = SQLDatabase(':memory:')
        db.add_table('test')
        db.add_column('test', 'a', data=np.arange(10, 20))
        db.add_column('test', 'b', data=np.arange(20, 30))
        return db

    def test_table_basic_properties(self):
        db = self.db
        table = db['test']
        self.assertEqual(table.name, 'test')
        self.assertEqual(table.db, db.db)
        self.assertEqual(table.column_names, ['a', 'b'])
        self.assertEqual(table.values, list(zip(np.arange(10, 20),
                                            np.arange(20, 30))))

    def test_table_select(self):
        db = self.db
        table = db['test']

        a = table.select()
        self.assertEqual(a, list(zip(np.arange(10, 20),
                                 np.arange(20, 30))))

        a = table.select(order='a')
        self.assertEqual(a, list(zip(np.arange(10, 20),
                                 np.arange(20, 30))))

        a = table.select(order='a', limit=2)
        self.assertEqual(a, [(10, 20), (11, 21)])

        a = table.select(order='a', limit=2, offset=2)
        self.assertEqual(a, [(12, 22), (13, 23)])

        a = table.select(order='a', where='a < 15')
        self.assertEqual(a, [(10, 20), (11, 21), (12, 22), (13, 23), (14, 24)])

        a = table.select(order='a', where='a < 15', limit=3)
        self.assertEqual(a, [(10, 20), (11, 21), (12, 22)])

        a = table.select(order='a', where='a < 15', limit=3, offset=2)
        self.assertEqual(a, [(12, 22), (13, 23), (14, 24)])

        a = table.select(columns=['a'], where='a < 15')
        self.assertEqual(a, [(10,), (11,), (12,), (13,), (14,)])

    def test_table_as_table(self):
        db = self.db
        table = db['test']

        a = table.as_table()
        self.assertIsInstance(a, Table)
        self.assertEqual(a.colnames, ['a', 'b'])
        self.assertEqual(a, Table(names=['a', 'b'], data=[np.arange(10, 20),
                                                      np.arange(20, 30)]))

    def test_table_as_table_empty(self):
        db = SQLDatabase(':memory:')
        db.add_table('test')
        table = db['test']

        a = table.as_table()
        self.assertIsInstance(a, Table)
        self.assertEqual(a.colnames, [])
        self.assertEqual(a, Table())

    def test_table_len(self):
        db = self.db
        table = db['test']
        self.assertEqual(len(table), 10)

    def test_table_iter(self):
        db = self.db
        table = db['test']

        v = 10
        for i in table:
            self.assertEqual(i, (v, v + 10))
            v += 1

    def test_table_contains(self):
        db = self.db
        table = db['test']

        self.assertFalse(10 in table)
        self.assertFalse(20 in table)
        self.assertFalse('c' in table)
        self.assertTrue('a' in table)
        self.assertTrue('b' in table)

    def test_table_repr(self):
        db = self.db
        table = db['test']
        i = hex(id(table))

        expect = "SQLTable 'test' in database ':memory:':"
        expect += "(2 columns x 10 rows)\n"
        expect += '\n'.join(table.as_table().__repr__().split('\n')[1:])
        self.assertIsInstance(table, SQLTable)
        self.assertEqual(repr(table), expect)

    def test_table_add_column(self):
        db = self.db
        table = db['test']

        table.add_column('c', data=np.arange(10, 20))
        self.assertEqual(table.column_names, ['a', 'b', 'c'])
        self.assertEqual(table.values, list(zip(np.arange(10, 20),
                                            np.arange(20, 30),
                                            np.arange(10, 20))))

        table.add_column('d', data=np.arange(20, 30))
        self.assertEqual(table.column_names, ['a', 'b', 'c', 'd'])
        self.assertEqual(table.values, list(zip(np.arange(10, 20),
                                            np.arange(20, 30),
                                            np.arange(10, 20),
                                            np.arange(20, 30))))

    def test_table_get_column(self):
        db = self.db
        table = db['test']

        a = table.get_column('a')
        self.assertIsInstance(a, SQLColumn)
        self.assertEqual(a.values, np.arange(10, 20))

        a = table.get_column('b')
        self.assertIsInstance(a, SQLColumn)
        self.assertEqual(a.values, np.arange(20, 30))

    def test_table_set_column(self):
        db = self.db
        table = db['test']

        table.set_column('a', np.arange(5, 15))
        self.assertEqual(table.column_names, ['a', 'b'])
        self.assertEqual(table.values, list(zip(np.arange(5, 15),
                                            np.arange(20, 30))))

    def test_table_set_column_invalid(self):
        db = self.db
        table = db['test']

        with self.assterRaises(ValueError):
            table.set_column('a', np.arange(5, 16))

        with self.assterRaises(KeyError):
            table.set_column('c', np.arange(5, 15))

    def test_table_add_row(self):
        db = self.db
        table = db['test']

        table.add_rows({'a': -1, 'b': -1})
        self.assertEqual(table.column_names, ['a', 'b'])
        self.assertEqual(len(table), 11)
        self.assertEqual(table[-1].values, (-1, -1))

        table.add_rows({'a': -2, 'c': -2}, add_columns=True)
        self.assertEqual(table.column_names, ['a', 'b', 'c'])
        self.assertEqual(len(table), 12)
        self.assertEqual(table[-1].values, (-2, None, -2))

        table.add_rows({'a': -3, 'd': -3}, add_columns=False)
        self.assertEqual(table.column_names, ['a', 'b', 'c'])
        self.assertEqual(len(table), 13)
        self.assertEqual(table[-1].values, (-3, None, None))

        # defult add_columns must be false
        table.add_rows({'a': -4, 'b': -4, 'c': -4, 'd': -4})
        self.assertEqual(table.column_names, ['a', 'b', 'c'])
        self.assertEqual(len(table), 14)
        self.assertEqual(table[-1].values, (-4, -4, -4))

    def test_table_add_row_invalid(self):
        db = self.db
        table = db['test']

        with self.assterRaises(ValueError):
            table.add_rows([1, 2, 3, 4])

        with self.assterRaises(TypeError):
            table.add_rows(2)

    def test_table_get_row(self):
        db = self.db
        table = db['test']

        a = table.get_row(0)
        self.assertIsInstance(a, SQLRow)
        self.assertEqual(a.values, (10, 20))

        a = table.get_row(1)
        self.assertIsInstance(a, SQLRow)
        self.assertEqual(a.values, (11, 21))

    def test_table_set_row(self):
        db = self.db
        table = db['test']

        table.set_row(0, {'a': 5, 'b': 15})
        expect = np.transpose([np.arange(10, 20), np.arange(20, 30)])
        expect[0] = [5, 15]
        self.assertEqual(table.column_names, ['a', 'b'])
        self.assertEqual(table.values, expect)

        expect[-1] = [-1, -1]
        table.set_row(-1, {'a': -1, 'b': -1})
        self.assertEqual(table.column_names, ['a', 'b'])
        self.assertEqual(table.values, expect)

        expect[-1] = [5, 5]
        table.set_row(-1, [5, 5])
        self.assertEqual(table.column_names, ['a', 'b'])
        self.assertEqual(table.values, expect)

    def test_table_set_row_invalid(self):
        db = self.db
        table = db['test']

        with self.assertRaises(IndexError):
            table.set_row(10, {'a': -1, 'b': -1})
        with self.assertRaises(IndexError):
            table.set_row(-11, {'a': -1, 'b': -1})

        with self.assertRaises(TypeError):
            table.set_row(0, 'a')

    def test_table_getitem_int(self):
        db = self.db
        table = db['test']
        self.assertIsInstance(table, SQLTable)

        self.assertEqual(table[0].values, (10, 20))
        self.assertEqual(table[-1].values, (19, 29))

        with self.assertRaises(IndexError):
            table[10]
        with self.assertRaises(IndexError):
            table[-11]

    def test_table_getitem_str(self):
        db = self.db
        table = db['test']
        self.assertIsInstance(table, SQLTable)

        self.assertEqual(table['a'].values, np.arange(10, 20))
        self.assertEqual(table['b'].values, np.arange(20, 30))

        with self.assertRaises(KeyError):
            table['c']

    def test_table_getitem_tuple(self):
        db = self.db
        table = db['test']
        self.assertIsInstance(table, SQLTable)

        self.assertEqual(table[('a',)].values, np.arange(10, 20))
        self.assertIsInstance(table[('a',)], SQLColumn)
        self.assertEqual(table[(1,)].values, (11, 21))
        self.assertIsInstance(table[(1,)], SQLRow)

        with self.assertRaises(KeyError):
            table[('c')]
        with self.assertRaises(IndexError):
            table[(11,)]

    def test_table_getitem_tuple_rowcol(self):
        db = self.db
        table = db['test']
        self.assertIsInstance(table, SQLTable)

        self.assertEqual(table['a', 0], 10)
        self.assertEqual(table['a', 1], 11)
        self.assertEqual(table['b', 0], 20)
        self.assertEqual(table['b', 1], 21)

        self.assertEqual(table[0, 'a'], 10)
        self.assertEqual(table[1, 'a'], 11)
        self.assertEqual(table[0, 'b'], 20)
        self.assertEqual(table[1, 'b'], 21)

        self.assertEqual(table['a', [0, 1, 2]], [10, 11, 12])
        self.assertEqual(table['b', [0, 1, 2]], [20, 21, 22])
        self.assertEqual(table[[0, 1, 2], 'b'], [20, 21, 22])
        self.assertEqual(table[[0, 1, 2], 'a'], [10, 11, 12])

        self.assertEqual(table['a', 2:5], [12, 13, 14])
        self.assertEqual(table['b', 2:5], [22, 23, 24])
        self.assertEqual(table[2:5, 'b'], [22, 23, 24])
        self.assertEqual(table[2:5, 'a'], [12, 13, 14])

        with self.assertRaises(KeyError):
            table['c', 0]
        with self.assertRaises(IndexError):
            table['a', 11]

        with self.assertRaises(KeyError):
            table[0, 0]
        with self.assertRaises(KeyError):
            table['b', 'a']
        with self.assertRaises(KeyError):
            table[0, 1, 2]
        with self.assertRaises(KeyError):
            table[0, 'a', 'b']

    def test_table_setitem_int(self):
        db = self.db
        table = db['test']
        self.assertIsInstance(table, SQLTable)

        table[0] = {'a': 5, 'b': 15}
        expect = np.transpose([np.arange(10, 20), np.arange(20, 30)])
        expect[0] = [5, 15]
        self.assertEqual(table.column_names, ['a', 'b'])
        self.assertEqual(table.values, expect)

        table[-1] = {'a': -1, 'b': -1}
        expect[-1] = [-1, -1]
        self.assertEqual(table.column_names, ['a', 'b'])
        self.assertEqual(table.values, expect)

        with self.assertRaises(IndexError):
            table[10] = {'a': -1, 'b': -1}
        with self.assertRaises(IndexError):
            table[-11] = {'a': -1, 'b': -1}

    def test_table_setitem_str(self):
        db = self.db
        table = db['test']
        self.assertIsInstance(table, SQLTable)

        table['a'] = np.arange(40, 50)
        expect = np.transpose([np.arange(40, 50), np.arange(20, 30)])
        self.assertEqual(table.column_names, ['a', 'b'])
        self.assertEqual(table.values, expect)

        table['b'] = np.arange(10, 20)
        expect = np.transpose([np.arange(40, 50), np.arange(10, 20)])
        self.assertEqual(table.column_names, ['a', 'b'])
        self.assertEqual(table.values, expect)

        with self.assertRaises(KeyError):
            table['c'] = np.arange(10, 20)

    def test_table_setitem_tuple(self):
        db = self.db
        table = db['test']
        self.assertIsInstance(table, SQLTable)

        table[('a',)] = np.arange(40, 50)
        expect = np.transpose([np.arange(40, 50), np.arange(20, 30)])
        self.assertEqual(table.column_names, ['a', 'b'])
        self.assertEqual(table.values, expect)

        table[(1,)] = {'a': -1, 'b': -1}
        expect[1] = [-1, -1]
        self.assertEqual(table.column_names, ['a', 'b'])
        self.assertEqual(table.values, expect)

        with self.assertRaises(KeyError):
            table[('c',)] = np.arange(10, 20)
        with self.assertRaises(IndexError):
            table[(11,)] = np.arange(10, 20)

    def test_table_setitem_tuple_multiple(self):
        db = self.db
        table = db['test']
        self.assertIsInstance(table, SQLTable)
        expect = np.transpose([np.arange(10, 20), np.arange(20, 30)])

        table[('a', 1)] = 57
        expect[1, 0] = 57
        table['b', -1] = 32
        expect[-1, 1] = 32
        table[0, 'a'] = -1
        expect[0, 0] = -1
        table[5, 'b'] = 99
        expect[5, 1] = 99
        table['a', 3:6] = -999
        expect[3:6, 0] = -999
        table['b', [2, 7]] = -888
        expect[[2, 7], 1] = -888
        self.assertEqual(table.values, expect)

        with self.assertRaises(KeyError):
            table[('c',)] = np.arange(10, 20)
        with self.assertRaises(IndexError):
            table[(11,)] = np.arange(10, 20)
        with self.assertRaises(KeyError):
            table['a', 'c'] = None
        with self.assertRaises(KeyError):
            table[2:5] = 2
        with self.assertRaises(KeyError):
            table[1, 2, 3] = 3

    def test_table_indexof(self):
        db = self.db
        table = db['test']
        self.assertEqual(table.index_of({'a': 15}), 5)
        self.assertEqual(table.index_of({'a': 50}), [])
        self.assertEqual(table.index_of('a < 13'), [0, 1, 2])

    def test_table_delete_row(self):
        db = self.db
        table = db['test']
        self.assertIsInstance(table, SQLTable)

        table.delete_row(0)
        expect = np.transpose([np.arange(11, 20), np.arange(21, 30)])
        self.assertEqual(table.column_names, ['a', 'b'])
        self.assertEqual(table.values, expect)

        table.delete_row(-1)
        expect = np.transpose([np.arange(11, 19), np.arange(21, 29)])
        self.assertEqual(table.column_names, ['a', 'b'])
        self.assertEqual(table.values, expect)

        with self.assertRaises(IndexError):
            table.delete_row(10)
        with self.assertRaises(IndexError):
            table.delete_row(-11)

    def test_table_delete_column(self):
        db = self.db
        table = db['test']
        self.assertIsInstance(table, SQLTable)

        table.delete_column('a')
        expect = np.transpose([np.arange(20, 30)])
        self.assertEqual(table.column_names, ['b'])
        self.assertEqual(table.values, expect)

        with self.assertRaises(KeyError):
            table.delete_column('a')
        with self.assertRaises(KeyError):
            table.delete_column('c')


class TestSQLColumn(TestCaseWithNumpyCompare):
    @property
    def db(self):
        db = SQLDatabase(':memory:')
        db.add_table('test')
        db.add_column('test', 'a', data=np.arange(10, 20))
        db.add_column('test', 'b', data=np.arange(20, 30))
        return db

    def test_column_basic_properties(self):
        db = self.db
        table = db['test']
        column = table['a']

        self.assertEqual(column.name, 'a')
        self.assertEqual(column.table, 'test')
        self.assertEqual(column.values, np.arange(10, 20))

    def test_column_len(self):
        db = self.db
        table = db['test']
        column = table['a']
        self.assertEqual(len(column), 10)

    def test_column_repr(self):
        db = self.db
        table = db['test']
        column = table['a']
        self.assertEqual(repr(column), "SQLColumn a in table 'test' (10 rows)")

    def test_column_contains(self):
        db = self.db
        table = db['test']
        column = table['a']
        self.assertTrue(15 in column)
        self.assertFalse(25 in column)

    def test_column_iter(self):
        db = self.db
        table = db['test']
        column = table['a']

        v = 10
        for i in column:
            self.assertEqual(i, v)
            v += 1

    def test_column_getitem_int(self):
        db = self.db
        table = db['test']
        column = table['a']

        self.assertEqual(column[0], 10)
        self.assertEqual(column[-1], 19)

        with self.assertRaises(IndexError):
            column[10]
        with self.assertRaises(IndexError):
            column[-11]

    def test_column_getitem_list(self):
        db = self.db
        table = db['test']
        column = table['a']

        self.assertEqual(column[[0, 1]], [10, 11])
        self.assertEqual(column[[-2, -1]], [18, 19])

        with self.assertRaises(IndexError):
            column[[10, 11]]
        with self.assertRaises(IndexError):
            column[[-11, -12]]

    def test_column_getitem_slice(self):
        db = self.db
        table = db['test']
        column = table['a']

        self.assertEqual(column[:2], [10, 11])
        self.assertEqual(column[-2:], [18, 19])
        self.assertEqual(column[2:5], [12, 13, 14])
        self.assertEqual(column[::-1], [19, 18, 17, 16, 15, 14, 13, 12, 11, 10])

    def test_column_getitem_tuple(self):
        db = self.db
        table = db['test']
        column = table['a']
        with self.assertRaises(IndexError):
            column[('a',)]
        with self.assertRaises(IndexError):
            column[(1,)]
        with self.assertRaises(IndexError):
            column[1, 2]

    def test_column_setitem_int(self):
        db = self.db
        table = db['test']
        column = table['a']

        column[0] = 5
        self.assertEqual(db.get_row('test', 0).values, [5, 20])

        column[-1] = -1
        self.assertEqual(db.get_row('test', -1).values, [-1, 29])

    def test_column_setitem_list_slice(self):
        db = self.db
        table = db['test']
        column = table['a']

        column[:] = -1
        self.assertEqual(db.get_column('test', 'a').values, [-1]*10)
        column[[2, 4]] = 2
        self.assertEqual(db.get_column('test', 'a').values, [-1, -1, 2, -1, 2,
                                                         -1, -1, -1, -1, -1])
    def test_column_setitem_invalid(self):
        db = self.db
        table = db['test']
        column = table['a']

        with self.assertRaises(IndexError):
            column[10] = 10
        with self.assertRaises(IndexError):
            column[-11] = 10
        with self.assertRaises(IndexError):
            column[2, 4] = [10, 11]


class TestSQLTableMapping(TestCaseWithNumpyCompare):
    @property
    def table(self):
        db = SQLDatabase(':memory:')
        db.add_table('test')
        db.add_column('test', 'a', data=np.arange(10, 20))
        db.add_column('test', 'b', data=np.arange(20, 30))

        db.add_table('mapping')
        db.add_column('mapping', 'keywords', ['key a', 'key-b'])
        db.add_column('mapping', 'columns', ['a', 'b'])

        map = SQLColumnMap(db, 'mapping', 'keywords', 'columns')
        return SQLTable(db, 'test', colmap=map)

    def test_table_select(self):
        table = self.table

        a = table.select()
        self.assertEqual(a, list(zip(np.arange(10, 20),
                                 np.arange(20, 30))))

        a = table.select(order='key a')
        self.assertEqual(a, list(zip(np.arange(10, 20),
                                 np.arange(20, 30))))

        a = table.select(order='key-b', limit=2)
        self.assertEqual(a, [(10, 20), (11, 21)])

        a = table.select(order='key a', limit=2, offset=2)
        self.assertEqual(a, [(12, 22), (13, 23)])

        a = table.select(order='key-b', where={'key a': 15})
        self.assertEqual(a, [(15, 25)])

    def test_table_column_names(self):
        table = self.table
        self.assertEqual(table.column_names, ['key a', 'key-b'])

    def test_table_getitem_str(self):
        table = self.table

        self.assertEqual(table['key a'].values, np.arange(10, 20))
        self.assertEqual(table['key-b'].values, np.arange(20, 30))

        with self.assertRaises(KeyError):
            table['c']

    def test_table_getitem_tuple(self):
        table = self.table
        self.assertEqual(table[('key a',)].values, np.arange(10, 20))
        self.assertIsInstance(table[('key a',)], SQLColumn)
        self.assertEqual(table[(1,)].values, (11, 21))
        self.assertIsInstance(table[(1,)], SQLRow)

        with self.assertRaises(KeyError):
            table[('c')]
        with self.assertRaises(IndexError):
            table[(11,)]

    def test_table_getitem_tuple_rowcol(self):
        table = self.table
        self.assertEqual(table['key a', 0], 10)
        self.assertEqual(table['key a', 1], 11)
        self.assertEqual(table['key-b', 0], 20)
        self.assertEqual(table['key-b', 1], 21)

        self.assertEqual(table[0, 'key a'], 10)
        self.assertEqual(table[1, 'key a'], 11)
        self.assertEqual(table[0, 'key-b'], 20)
        self.assertEqual(table[1, 'key-b'], 21)

        self.assertEqual(table['key a', [0, 1, 2]], [10, 11, 12])
        self.assertEqual(table['key-b', [0, 1, 2]], [20, 21, 22])
        self.assertEqual(table[[0, 1, 2], 'key-b'], [20, 21, 22])
        self.assertEqual(table[[0, 1, 2], 'key a'], [10, 11, 12])

        self.assertEqual(table['key a', 2:5], [12, 13, 14])
        self.assertEqual(table['key-b', 2:5], [22, 23, 24])
        self.assertEqual(table[2:5, 'key-b'], [22, 23, 24])
        self.assertEqual(table[2:5, 'key a'], [12, 13, 14])

        with self.assertRaises(KeyError):
            table['c', 0]
        with self.assertRaises(IndexError):
            table['key a', 11]

        with self.assertRaises(KeyError):
            table[0, 0]
        with self.assertRaises(KeyError):
            table['key-b', 'key a']
        with self.assertRaises(KeyError):
            table[0, 1, 2]
        with self.assertRaises(KeyError):
            table[0, 'key a', 'key-b']

    def test_table_set_row(self):
        table = self.table

        table.set_row(0, {'key a': 5, 'key-b': 15})
        expect = np.transpose([np.arange(10, 20), np.arange(20, 30)])
        expect[0] = [5, 15]
        self.assertEqual(table.column_names, ['key a', 'key-b'])
        self.assertEqual(table.values, expect)

        expect[-1] = [-1, -1]
        table.set_row(-1, {'key a': -1, 'key-b': -1})
        self.assertEqual(table.column_names, ['key a', 'key-b'])
        self.assertEqual(table.values, expect)

        expect[-1] = [5, 5]
        table.set_row(-1, [5, 5])
        self.assertEqual(table.column_names, ['key a', 'key-b'])
        self.assertEqual(table.values, expect)

    def test_table_add_row(self):
        table = self.table

        table.add_rows({'key a': -1, 'key-b': -1})
        self.assertEqual(table.column_names, ['key a', 'key-b'])
        self.assertEqual(len(table), 11)
        self.assertEqual(table[-1].values, (-1, -1))

        table.add_rows({'key a': -2, 'key!c': -2}, add_columns=True)
        self.assertEqual(table.column_names, ['key a', 'key-b', 'key!c'])
        self.assertEqual(len(table), 12)
        self.assertEqual(table[-1].values, (-2, None, -2))

        table.add_rows({'key a': -3, 'key_d': -3}, add_columns=False)
        self.assertEqual(table.column_names, ['key a', 'key-b', 'key!c'])
        self.assertEqual(len(table), 13)
        self.assertEqual(table[-1].values, (-3, None, None))

        # defult add_columns must be false
        table.add_rows({'key a': -4, 'key-b': -4, 'key!c': -4, 'key_d': -4})
        self.assertEqual(table.column_names, ['key a', 'key-b', 'key!c'])
        self.assertEqual(len(table), 14)
        self.assertEqual(table[-1].values, (-4, -4, -4))

    def test_table_get_column(self):
        table = self.table

        a = table.get_column('key a')
        self.assertIsInstance(a, SQLColumn)
        self.assertEqual(a.values, np.arange(10, 20))
        self.assertEqual(a.name, 'a')

        a = table.get_column('key-b')
        self.assertIsInstance(a, SQLColumn)
        self.assertEqual(a.values, np.arange(20, 30))
        self.assertEqual(a.name, 'b')

    def test_table_set_column(self):
        table = self.table

        table.set_column('key a', np.arange(5, 15))
        self.assertEqual(table.column_names, ['key a', 'key-b'])
        self.assertEqual(table.values, list(zip(np.arange(5, 15),
                                            np.arange(20, 30))))

    def test_table_set_column_invalid(self):
        table = self.table

        with self.assterRaises(ValueError):
            table.set_column('key a', np.arange(5, 16))

        with self.assterRaises(KeyError):
            table.set_column('key!c', np.arange(5, 15))

    def test_table_add_column(self):
        table = self.table

        table.add_column('key!c', data=np.arange(10, 20))
        self.assertEqual(table.column_names, ['key a', 'key-b', 'key!c'])
        self.assertEqual(table.values, list(zip(np.arange(10, 20),
                                            np.arange(20, 30),
                                            np.arange(10, 20))))

        table.add_column('key_d', data=np.arange(20, 30))
        self.assertEqual(table.column_names, ['key a', 'key-b', 'key!c', 'key_d'])
        self.assertEqual(table.values, list(zip(np.arange(10, 20),
                                            np.arange(20, 30),
                                            np.arange(10, 20),
                                            np.arange(20, 30))))

    def test_table_contains(self):
        table = self.table

        self.assertFalse(10 in table)
        self.assertFalse(20 in table)
        self.assertFalse('key!c' in table)
        self.assertTrue('key a' in table)
        self.assertTrue('key-b' in table)

    def test_table_as_table_empty(self):
        db = SQLDatabase(':memory:')
        db.add_table('test')
        db.add_table('mapping')
        db.add_column('mapping', 'keywords')
        db.add_column('mapping', 'columns')
        table = SQLTable(db, 'test',
                         SQLColumnMap(db, 'mapping', 'keywords', 'columns'))

        a = table.as_table()
        self.assertIsInstance(a, Table)
        self.assertEqual(a.colnames, [])
        self.assertEqual(a, Table())

    def test_table_as_table(self):
        table = self.table

        a = table.as_table()
        self.assertIsInstance(a, Table)
        self.assertEqual(a.colnames, ['key a', 'key-b'])
        self.assertEqual(a, Table(names=['key a', 'key-b'],
                              data=[np.arange(10, 20), np.arange(20, 30)]))

    def test_table_setitem_int(self):
        table = self.table

        table[0] = {'key a': 5, 'key-b': 15}
        expect = np.transpose([np.arange(10, 20), np.arange(20, 30)])
        expect[0] = [5, 15]
        self.assertEqual(table.column_names, ['key a', 'key-b'])
        self.assertEqual(table.values, expect)

        table[-1] = {'key a': -1, 'key-b': -1}
        expect[-1] = [-1, -1]
        self.assertEqual(table.column_names, ['key a', 'key-b'])
        self.assertEqual(table.values, expect)

        with self.assertRaises(IndexError):
            table[10] = {'key a': -1, 'key-b': -1}
        with self.assertRaises(IndexError):
            table[-11] = {'key a': -1, 'key-b': -1}

    def test_table_setitem_str(self):
        table = self.table

        table['key a'] = np.arange(40, 50)
        expect = np.transpose([np.arange(40, 50), np.arange(20, 30)])
        self.assertEqual(table.column_names, ['key a', 'key-b'])
        self.assertEqual(table.values, expect)

        table['key-b'] = np.arange(10, 20)
        expect = np.transpose([np.arange(40, 50), np.arange(10, 20)])
        self.assertEqual(table.column_names, ['key a', 'key-b'])
        self.assertEqual(table.values, expect)

        with self.assertRaises(KeyError):
            table['c'] = np.arange(10, 20)

    def test_table_setitem_tuple(self):
        table = self.table

        table[('key a',)] = np.arange(40, 50)
        expect = np.transpose([np.arange(40, 50), np.arange(20, 30)])
        self.assertEqual(table.column_names, ['key a', 'key-b'])
        self.assertEqual(table.values, expect)

        table[(1,)] = {'key a': -1, 'key-b': -1}
        expect[1] = [-1, -1]
        self.assertEqual(table.column_names, ['key a', 'key-b'])
        self.assertEqual(table.values, expect)

    def test_table_setitem_tuple_multiple(self):
        table = self.table
        expect = np.transpose([np.arange(10, 20), np.arange(20, 30)])

        table[('key a', 1)] = 57
        expect[1, 0] = 57
        table['key-b', -1] = 32
        expect[-1, 1] = 32
        table[0, 'key a'] = -1
        expect[0, 0] = -1
        table[5, 'key-b'] = 99
        expect[5, 1] = 99
        table['key a', 3:6] = -999
        expect[3:6, 0] = -999
        table['key-b', [2, 7]] = -888
        expect[[2, 7], 1] = -888
        self.assertEqual(table.values, expect)

    def test_table_indexof(self):
        table = self.table
        self.assertEqual(table.index_of({'key a': 15}), 5)
        self.assertEqual(table.index_of({'key a': 50}), [])
        with self.assertRaises(TypeError):
            self.assertEqual(table.index_of('"key a" < 13'), [0, 1, 2])
