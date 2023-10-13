import unittest
from dbastable.where import Where, _WhereParserMixin
from dbastable._sanitizer import _SanitizerMixin
from dbastable import SQLDatabase

from dbastable.tests.mixins import TestCaseWithNumpyCompare


class _WhereParser(_WhereParserMixin, _SanitizerMixin):
    def column_names(self, table):
        return ['a', 'b', 'c']


class TestWhere(unittest.TestCase):
    def test_where_equal(self):
        w = Where('a', '=', 1)
        self.assertEqual(w.to_sql[0], 'a = ?')
        self.assertIsInstance(w.to_sql[1], list)
        self.assertEqual(w.to_sql[1], [1])

    def test_where_equal_error(self):
        with self.assertRaises(ValueError):
            Where('a', '=', [1, 2])
        with self.assertRaises(ValueError):
            Where('a', '=', [])

    def test_where_not_equal(self):
        w = Where('a', '!=', 1)
        self.assertEqual(w.to_sql[0], 'a != ?')
        self.assertIsInstance(w.to_sql[1], list)
        self.assertEqual(w.to_sql[1], [1])

    def test_where_not_equal_error(self):
        with self.assertRaises(ValueError):
            Where('a', '!=', [1, 2])
        with self.assertRaises(ValueError):
            Where('a', '!=', [])

    def test_where_greater_than(self):
        w = Where('a', '>', 1)
        self.assertEqual(w.to_sql[0], 'a > ?')
        self.assertIsInstance(w.to_sql[1], list)
        self.assertEqual(w.to_sql[1], [1])

    def test_where_greater_than_error(self):
        with self.assertRaises(ValueError):
            Where('a', '>', [1, 2])
        with self.assertRaises(ValueError):
            Where('a', '>', [])

    def test_where_greater_than_or_equal(self):
        w = Where('a', '>=', 1)
        self.assertEqual(w.to_sql[0], 'a >= ?')
        self.assertIsInstance(w.to_sql[1], list)
        self.assertEqual(w.to_sql[1], [1])

    def test_where_greater_than_or_equal_error(self):
        with self.assertRaises(ValueError):
            Where('a', '>=', [1, 2])
        with self.assertRaises(ValueError):
            Where('a', '>=', [])

    def test_where_less_than(self):
        w = Where('a', '<', 1)
        self.assertEqual(w.to_sql[0], 'a < ?')
        self.assertIsInstance(w.to_sql[1], list)
        self.assertEqual(w.to_sql[1], [1])

    def test_where_less_than_error(self):
        with self.assertRaises(ValueError):
            Where('a', '<', [1, 2])
        with self.assertRaises(ValueError):
            Where('a', '<', [])

    def test_where_less_than_or_equal(self):
        w = Where('a', '<=', 1)
        self.assertEqual(w.to_sql[0], 'a <= ?')
        self.assertIsInstance(w.to_sql[1], list)
        self.assertEqual(w.to_sql[1], [1])

    def test_where_less_than_or_equal_error(self):
        with self.assertRaises(ValueError):
            Where('a', '<=', [1, 2])
        with self.assertRaises(ValueError):
            Where('a', '<=', [])

    def test_where_like(self):
        w = Where('a', 'like', 'b')
        self.assertEqual(w.to_sql[0], 'a LIKE ?')
        self.assertIsInstance(w.to_sql[1], list)
        self.assertEqual(w.to_sql[1], ['b'])

    def test_where_like_error(self):
        with self.assertRaises(ValueError):
            Where('a', 'like', [1, 2])
        with self.assertRaises(ValueError):
            Where('a', 'like', [])

    def test_where_in(self):
        w = Where('a', 'in', [1, 2, 3])
        self.assertEqual(w.to_sql[0], 'a IN (?, ?, ?)')
        self.assertIsInstance(w.to_sql[1], list)
        self.assertEqual(w.to_sql[1], [1, 2, 3])

    def test_where_in_error(self):
        with self.assertRaises(ValueError):
            Where('a', 'in', [])

    def test_where_not_in(self):
        w = Where('a', 'not in', [1, 2, 3])
        self.assertEqual(w.to_sql[0], 'a NOT IN (?, ?, ?)')
        self.assertIsInstance(w.to_sql[1], list)
        self.assertEqual(w.to_sql[1], [1, 2, 3])

    def test_where_not_in_error(self):
        with self.assertRaises(ValueError):
            Where('a', 'not in', [])

    def test_where_is(self):
        w = Where('a', 'is', None)
        self.assertEqual(w.to_sql[0], 'a IS ?')
        self.assertIsInstance(w.to_sql[1], list)
        self.assertEqual(w.to_sql[1], [None])

    def test_where_is_not(self):
        w = Where('a', 'is not', None)
        self.assertEqual(w.to_sql[0], 'a IS NOT ?')
        self.assertIsInstance(w.to_sql[1], list)
        self.assertEqual(w.to_sql[1], [None])

    def test_where_between(self):
        w = Where('a', 'between', [1, 2])
        self.assertEqual(w.to_sql[0], 'a BETWEEN ? AND ?')
        self.assertIsInstance(w.to_sql[1], list)
        self.assertEqual(w.to_sql[1], [1, 2])

    def test_where_between_error(self):
        with self.assertRaises(ValueError):
            Where('a', 'between', [1])
        with self.assertRaises(ValueError):
            Where('a', 'between', [1, 2, 3])
        with self.assertRaises(ValueError):
            Where('a', 'between', [])

    def test_where_not_between(self):
        w = Where('a', 'not between', [1, 2])
        self.assertEqual(w.to_sql[0], 'a NOT BETWEEN ? AND ?')
        self.assertIsInstance(w.to_sql[1], list)
        self.assertEqual(w.to_sql[1], [1, 2])

    def test_where_not_between_error(self):
        with self.assertRaises(ValueError):
            Where('a', 'not between', [1])
        with self.assertRaises(ValueError):
            Where('a', 'not between', [1, 2, 3])
        with self.assertRaises(ValueError):
            Where('a', 'not between', [])


class TestParseWhere(unittest.TestCase):
    def test_parse_where_none(self):
        w = _WhereParser()
        self.assertEqual(w._parse_where(None), (None, None))

    def test_parse_where_single(self):
        w = _WhereParser()
        self.assertEqual(w._parse_where({'a': 1}), ('a = ?', [1]))

    def test_parse_where_single_error(self):
        w = _WhereParser()
        with self.assertRaises(TypeError):
            w._parse_where({'a': [1, 2]})
        with self.assertRaises(TypeError):
            w._parse_where({'a': []})

    def test_parse_where_single_where(self):
        w = _WhereParser()
        self.assertEqual(w._parse_where(Where('a', '=', 1)),
                         ('a = ?', [1]))


class TestWhereSelect(TestCaseWithNumpyCompare):
    def test_select_where_none(self):
        db = SQLDatabase(':memory:')
        db.add_table('test')
        db.add_column('test', 'a', [1, 2, 3, 4, 5])
        db.add_column('test', 'b', ['a', 'b', 'c', 'd', 'e'])

        sel = db.select('test', columns=['a'], where=None)
        self.assertEqualArray(sel, [[1], [2], [3], [4], [5]])
