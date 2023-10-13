# Licensed under a 3-clause BSD style license - see LICENSE.rst
# flake8: noqa: F403, F405

from dbastable import Where, SQLDatabase
from dbastable.tests.mixins import TestCaseWithNumpyCompare

# Here we will put the general tests that include non-conformant column names

class TestNonConformantColumns(TestCaseWithNumpyCompare):
    def test_create_column(self):
        db = SQLDatabase(':memory:', alow_b32_colnames=True)
        db.add_table('test')
        db.add_column('test', 'test!1 ')
        db.add_column('test', 'test@ 2')

        # check if column_names report the correct names of the columns
        self.assertEqual(db.column_names('test'), ['test!1 ', 'test@ 2'])
        self.assertEqual(db.column_names('test', do_not_decode=True),
                         ['__b32__ORSXG5BBGEQA', '__b32__ORSXG5CAEAZA'])

    def test_create_column_with_data(self):
        db = SQLDatabase(':memory:', alow_b32_colnames=True)
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
        db = SQLDatabase(':memory:', alow_b32_colnames=True)
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
        db = SQLDatabase(':memory:', alow_b32_colnames=True)
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
        db = SQLDatabase(':memory:', alow_b32_colnames=True)
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
        db = SQLDatabase(':memory:', alow_b32_colnames=True)
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
        db = SQLDatabase(':memory:', alow_b32_colnames=True)
        db.add_table('test')
        db.add_column('test', 'test!1 ', data=[1, 2, 3])
        db.add_column('test', 'test@ 2', data=[4, 5, 6])

        sel = db.select('test', where=Where('test!1 ', '>', 1))
        self.assertEqual(sel, [(2, 5), (3, 6)])

    def test_column_where_dict(self):
        db = SQLDatabase(':memory:', alow_b32_colnames=True)
        db.add_table('test')
        db.add_column('test', 'test!1 ', data=[1, 2, 3])
        db.add_column('test', 'test@ 2', data=[4, 5, 6])

        sel = db.select('test', where={'test!1 ': 2})
        self.assertEqual(sel, [(2, 5)])
