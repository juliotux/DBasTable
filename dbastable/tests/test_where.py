import unittest
from dbastable.where import Where


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
