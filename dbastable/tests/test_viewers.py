# Licensed under a 3-clause BSD style license - see LICENSE.rst
# flake8: noqa: F403, F405

import numpy as np
from dbastable import SQLColumn, SQLRow, SQLTable, SQLDatabase, Where
from astropy.table import Table

from dbastable.tests.mixins import TestCaseWithNumpyCompare


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
        self.assertEqual(row.values, (10, 20))
        self.assertIsInstance(row.values, tuple)
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
        self.assertEqualArray(db['test']['a'].values,
                              [1, 11, 12, 13, 14, 15, 16, 17, 18, 19])
        self.assertEqualArray(db['test']['b'].values,
                              [1, 21, 22, 23, 24, 25, 26, 27, 28, 29])

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
        self.assertEqual(
            repr(row), "SQLRow 0 in table 'test' {'a': 10, 'b': 20}")


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

        a = table.select(order='a', where=Where('a', '<', 15))
        self.assertEqual(a, [(10, 20), (11, 21), (12, 22), (13, 23), (14, 24)])

        a = table.select(order='a', where=Where('a', '<', 15), limit=3)
        self.assertEqual(a, [(10, 20), (11, 21), (12, 22)])

        a = table.select(order='a', where=Where('a', '<', 15), limit=3, offset=2)
        self.assertEqual(a, [(12, 22), (13, 23), (14, 24)])

        a = table.select(columns=['a'], where=Where('a', '<', 15))
        self.assertEqual(a, [(10,), (11,), (12,), (13,), (14,)])

    def test_table_as_table(self):
        db = self.db
        table = db['test']

        a = table.as_table()
        self.assertIsInstance(a, Table)
        self.assertEqual(a.colnames, ['a', 'b'])
        self.assertEqualArray(a, Table(names=['a', 'b'],
                                       data=[np.arange(10, 20),
                                             np.arange(20, 30)]))

    def test_table_as_table_empty(self):
        db = SQLDatabase(':memory:')
        db.add_table('test')
        table = db['test']

        a = table.as_table()
        self.assertIsInstance(a, Table)
        self.assertEqual(a.colnames, [])
        self.assertEqual(len(a), 0)

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
        self.assertEqualArray(a.values, np.arange(10, 20))

        a = table.get_column('b')
        self.assertIsInstance(a, SQLColumn)
        self.assertEqualArray(a.values, np.arange(20, 30))

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

        with self.assertRaises(ValueError):
            table.set_column('a', np.arange(5, 16))

        with self.assertRaises(KeyError):
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

        with self.assertRaises(KeyError):
            # non-existing columns must raise error
            table.add_rows({'a': -3, 'd': -3}, add_columns=False)

        # defult add_columns must be false
        with self.assertRaises(KeyError):
            # non-existing columns must raise error
            table.add_rows({'a': -4, 'd': -4})

    def test_table_add_row_invalid(self):
        db = self.db
        table = db['test']

        with self.assertRaises(ValueError):
            table.add_rows([1, 2, 3, 4])

        with self.assertRaises(TypeError):
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
        self.assertEqualArray(table.values, expect)

        expect[-1] = [-1, -1]
        table.set_row(-1, {'a': -1, 'b': -1})
        self.assertEqual(table.column_names, ['a', 'b'])
        self.assertEqualArray(table.values, expect)

        expect[-1] = [5, 5]
        table.set_row(-1, [5, 5])
        self.assertEqual(table.column_names, ['a', 'b'])
        self.assertEqualArray(table.values, expect)

    def test_table_set_row_invalid(self):
        db = self.db
        table = db['test']

        with self.assertRaises(IndexError):
            table.set_row(10, {'a': -1, 'b': -1})
        with self.assertRaises(IndexError):
            table.set_row(-11, {'a': -1, 'b': -1})

        with self.assertRaises(TypeError):
            table.set_row(0, 'a')

        with self.assertRaises(KeyError):
            # non-existing columns must raise error
            table.set_row(0, {'a': -1, 'c': -1})

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

        self.assertEqualArray(table['a'].values, np.arange(10, 20))
        self.assertEqualArray(table['b'].values, np.arange(20, 30))

        with self.assertRaises(KeyError):
            table['c']

    def test_table_getitem_tuple(self):
        db = self.db
        table = db['test']
        self.assertIsInstance(table, SQLTable)

        self.assertEqualArray(table[('a',)].values, np.arange(10, 20))
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
        self.assertEqualArray(table.values, expect)

        table[-1] = {'a': -1, 'b': -1}
        expect[-1] = [-1, -1]
        self.assertEqual(table.column_names, ['a', 'b'])
        self.assertEqualArray(table.values, expect)

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
        self.assertEqualArray(table.values, expect)

        table['b'] = np.arange(10, 20)
        expect = np.transpose([np.arange(40, 50), np.arange(10, 20)])
        self.assertEqual(table.column_names, ['a', 'b'])
        self.assertEqualArray(table.values, expect)

        # if the column does not exists, it will be created
        table['c'] = np.arange(10, 20)
        self.assertEqualArray(table['c'].values, np.arange(10, 20))

    def test_table_setitem_tuple(self):
        db = self.db
        table = db['test']
        self.assertIsInstance(table, SQLTable)

        table[('a',)] = np.arange(40, 50)
        expect = np.transpose([np.arange(40, 50), np.arange(20, 30)])
        self.assertEqual(table.column_names, ['a', 'b'])
        self.assertEqualArray(table.values, expect)

        table[(1,)] = {'a': -1, 'b': -1}
        expect[1] = [-1, -1]
        self.assertEqual(table.column_names, ['a', 'b'])
        self.assertEqualArray(table.values, expect)

        # if the column does not exists, it will be created
        table[('c',)] = np.arange(10, 20)
        self.assertEqualArray(table['c'].values, np.arange(10, 20))

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
        self.assertEqualArray(table.values, expect)

        # if the column does not exists, it will be created
        table[('c',)] = np.arange(10, 20)
        self.assertEqualArray(table['c'].values, np.arange(10, 20))

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
        self.assertEqual(table.index_of(Where('a', '<', 13)), [0, 1, 2])

    def test_table_delete_row(self):
        db = self.db
        table = db['test']
        self.assertIsInstance(table, SQLTable)

        table.delete_row(0)
        expect = np.transpose([np.arange(11, 20), np.arange(21, 30)])
        self.assertEqual(table.column_names, ['a', 'b'])
        self.assertEqualArray(table.values, expect)

        table.delete_row(-1)
        expect = np.transpose([np.arange(11, 19), np.arange(21, 29)])
        self.assertEqual(table.column_names, ['a', 'b'])
        self.assertEqualArray(table.values, expect)

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
        self.assertEqualArray(table.values, expect)

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
        self.assertEqualArray(column.values, np.arange(10, 20))

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
        self.assertEqual(
            column[::-1], [19, 18, 17, 16, 15, 14, 13, 12, 11, 10])

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
        self.assertEqualArray(db.get_row('test', 0).values, [5, 20])

        column[-1] = -1
        self.assertEqualArray(db.get_row('test', -1).values, [-1, 29])

    def test_column_setitem_list_slice(self):
        db = self.db
        table = db['test']
        column = table['a']

        column[:] = -1
        self.assertEqual(db.get_column('test', 'a').values, [-1]*10)
        column[[2, 4]] = 2
        self.assertEqual(db.get_column('test', 'a').values,
                         [-1, -1, 2, -1, 2, -1, -1, -1, -1, -1])

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
