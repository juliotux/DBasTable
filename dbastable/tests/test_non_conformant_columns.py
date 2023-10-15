# Licensed under a 3-clause BSD style license - see LICENSE.rst
# flake8: noqa: F403, F405

from dbastable import Where, SQLDatabase
from dbastable.tests.mixins import TestCaseWithNumpyCompare

# Here we will put the general tests that include non-conformant column names

class TestNonConformantColumns(TestCaseWithNumpyCompare):
    def test_create_column(self):
        db = SQLDatabase(':memory:', allow_b32_colnames=True)
        db.add_table('test')
        db.add_column('test', 'test!1 ')
        db.add_column('test', 'test@ 2')

        # check if column_names report the correct names of the columns
        self.assertEqual(db.column_names('test'), ['test!1 ', 'test@ 2'])
        self.assertEqual(db.column_names('test', do_not_decode=True),
                         ['__b32__ORSXG5BBGEQA', '__b32__ORSXG5CAEAZA'])

    def test_create_column_with_data(self):
        db = SQLDatabase(':memory:', allow_b32_colnames=True)
        db.add_table('test')
        db.add_column('test', 'test!1 ', data=[1, 2, 3])
        db.add_column('test', 'test@ 2', data=[4, 5, 6])

        # check if column_names report the correct names of the columns
        self.assertEqual(db.column_names('test'), ['test!1 ', 'test@ 2'])
        self.assertEqual(db.column_names('test', do_not_decode=True),
                         ['__b32__ORSXG5BBGEQA', '__b32__ORSXG5CAEAZA'])

        # check if the data is correct
        self.assertEqual(db.get_column('test', 'test!1 ').values, [1, 2, 3])
        self.assertEqual(db.get_column('test', 'test@ 2').values, [4, 5, 6])

    def test_getitem_column(self):
        db = SQLDatabase(':memory:', allow_b32_colnames=True)
        db.add_table('test')
        db.add_column('test', 'test!1 ', data=[1, 2, 3])
        db.add_column('test', 'test@ 2', data=[4, 5, 6])

        # check if the data is correct
        self.assertEqual(db['test', 'test!1 '].values, [1, 2, 3])
        self.assertEqual(db['test', 'test@ 2'].values, [4, 5, 6])
        self.assertEqual(db['test', 'test!1 '].name, 'test!1 ')

        # Also case insensitive
        self.assertEqual(db['test', 'TEST!1 '].values, [1, 2, 3])
        self.assertEqual(db['test', 'TEST@ 2'].values, [4, 5, 6])
        self.assertEqual(db['test', 'Test!1 '].name, 'test!1 ')

    def test_setitem_column(self):
        db = SQLDatabase(':memory:', allow_b32_colnames=True)
        db.add_table('test')
        db.add_column('test', 'test!1 ', data=[1, 2, 3])
        db.add_column('test', 'test@ 2', data=[4, 5, 6])

        # check if the data is correct
        db['test', 'test!1 '] = [7, 8, 9]
        db['test', 'test@ 2'] = [10, 11, 12]
        self.assertEqual(db['test', 'test!1 '].values, [7, 8, 9])
        self.assertEqual(db['test', 'test@ 2'].values, [10, 11, 12])

        # Also case insensitive
        db['test', 'TEST!1 '] = [8, 8, 9]
        db['test', 'TEST@ 2'] = [8, 11, 12]
        self.assertEqual(db['test', 'test!1 '].values, [8, 8, 9])
        self.assertEqual(db['test', 'test@ 2'].values, [8, 11, 12])

    def test_column_from_row(self):
        db = SQLDatabase(':memory:', allow_b32_colnames=True)
        db.add_table('test')
        db.add_column('test', 'test!1 ', data=[1, 2, 3])
        db.add_column('test', 'test@ 2', data=[4, 5, 6])

        # check if the data is correct
        r = db['test'][1]
        self.assertEqual(r['test!1 '], 2)
        self.assertEqual(r['test@ 2'], 5)
        self.assertEqual(r['test!1 '], r[0])
        self.assertEqual(r['test@ 2'], r[1])

        # also case insensitive
        self.assertEqual(r['tEst!1 '], 2)
        self.assertEqual(r['tesT@ 2'], 5)

    def test_column_from_table(self):
        db = SQLDatabase(':memory:', allow_b32_colnames=True)
        db.add_table('test')
        db.add_column('test', 'test!1 ', data=[1, 2, 3])
        db.add_column('test', 'test@ 2', data=[4, 5, 6])

        # check if the data is correct
        r = db['test']
        self.assertEqual(r['test!1 '].values, [1, 2, 3])
        self.assertEqual(r['test@ 2'].values, [4, 5, 6])

        # also case insensitive
        self.assertEqual(r['tEst!1 '].values, [1, 2, 3])
        self.assertEqual(r['tesT@ 2'].values, [4, 5, 6])

    def test_column_where(self):
        db = SQLDatabase(':memory:', allow_b32_colnames=True)
        db.add_table('test')
        db.add_column('test', 'test!1 ', data=[1, 2, 3])
        db.add_column('test', 'test@ 2', data=[4, 5, 6])

        sel = db.select('test', where=Where('test!1 ', '>', 1))
        self.assertEqual(sel, [(2, 5), (3, 6)])

    def test_column_where_dict(self):
        db = SQLDatabase(':memory:', allow_b32_colnames=True)
        db.add_table('test')
        db.add_column('test', 'test!1 ', data=[1, 2, 3])
        db.add_column('test', 'test@ 2', data=[4, 5, 6])

        sel = db.select('test', where={'test!1 ': 2})
        self.assertEqual(sel, [(2, 5)])

    def test_add_row_dict(self):
        db = SQLDatabase(':memory:', allow_b32_colnames=True)
        db.add_table('test')
        db.add_column('test', 'test!1 ', data=[1, 2, 3])
        db.add_column('test', 'test@ 2', data=[4, 5, 6])

        db.add_rows('test', {'test!1 ': 4, 'test@ 2': 7})
        self.assertEqual(db['test']['test!1 '].values, [1, 2, 3, 4])
        self.assertEqual(db['test']['test@ 2'].values, [4, 5, 6, 7])

        # also case insensitive
        db.add_rows('test', {'TEST!1 ': 5, 'TEST@ 2': 8})
        self.assertEqual(db['test']['test!1 '].values, [1, 2, 3, 4, 5])
        self.assertEqual(db['test']['test@ 2'].values, [4, 5, 6, 7, 8])

    def test_add_row_dict_add_missing_columns(self):
        db = SQLDatabase(':memory:', allow_b32_colnames=True)
        db.add_table('test')
        db.add_rows('test', {'test!1 ': 4, 'test@ 2': 7},
                    add_columns=True)
        self.assertEqual(db['test']['test!1 '].values, [4])
        self.assertEqual(db['test']['test@ 2'].values, [7])

        # also case insensitive
        db.add_rows('test', {'TEST!1 ': 5, 'TEST@ 2': 8})
        self.assertEqual(db['test']['test!1 '].values, [4, 5])
        self.assertEqual(db['test']['test@ 2'].values, [7, 8])

    def test_add_columns_with_data(self):
        from dbastable import SQLDatabase
        db = SQLDatabase(allow_b32_colnames=True)
        db.add_table('my_table')
        db.add_column('my_table', '1test', data=[1, 2, 3])
        db.add_column('my_table', 'test column', data=[1, 2, 3])
        db.add_column('my_table', 'test!with!exclamation!points', data=[1, 2, 3])
        db.add_column('my_table', 'test@with@at@signs', data=[1, 2, 3])

        self.assertEqual(db['my_table']['1test'].values, [1, 2, 3])
        self.assertEqual(db['my_table']['test column'].values, [1, 2, 3])
        self.assertEqual(db['my_table']['test!with!exclamation!points'].values, [1, 2, 3])
        self.assertEqual(db['my_table']['test@with@at@signs'].values, [1, 2, 3])

        db.add_column('my_table', 'test')

        self.assertEqual(db['my_table']['1test'].values, [1, 2, 3])
        self.assertEqual(db['my_table']['test column'].values, [1, 2, 3])
        self.assertEqual(db['my_table']['test!with!exclamation!points'].values, [1, 2, 3])
        self.assertEqual(db['my_table']['test@with@at@signs'].values, [1, 2, 3])

        sel = db.select('my_table', where=Where('1test', '>', 1))
        self.assertEqual(sel, [(2, 2, 2, 2, None), (3, 3, 3, 3, None)])

