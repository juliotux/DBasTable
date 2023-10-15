# Licensed under a 3-clause BSD style license - see LICENSE.rst
# flake8: noqa: F403, F405

from dbastable._sanitizer import _SanitizerMixin
from dbastable.tests.mixins import TestCaseWithNumpyCompare


class _Sanitizer(_SanitizerMixin):
    def __init__(self, allow_b32_colnames=False):
        self._allow_b32_colnames = allow_b32_colnames

    def column_names(self, table, do_not_decode=False):
        if do_not_decode:
            return ['test1', 'test2',
                    '__b32__ORSXG5BNGI']  # 'test-2'
        return ['test1', 'test2', 'test-2']


class TestSanitizeColnames(TestCaseWithNumpyCompare):
    def test_sanitize_string(self):
        s = _Sanitizer(False)
        for i in ['test-2', 'test!2', 'test@2', 'test#2', 'test$2',
                  'test&2', 'test*2', 'test(2)', 'test)2', 'test[2]', 'test]2',
                  'test{2}', 'test}2', 'test|2', 'test\\2', 'test^2', 'test~2'
                  'test"2', 'test\'2', 'test`2', 'test<2', 'test>2', 'test=2',
                  'test,2', 'test;2', 'test:2', 'test?2', 'test/2']:
            with self.assertRaises(ValueError):
                s._sanitize_colnames(i)

        for i in ['test', 'test_1', 'test_1_2', 'test_1_2', 'Test', 'Test_1']:
            self.assertEqual(s._sanitize_colnames(i), i.lower())

    def test_sanitize_list(self):
        s = _Sanitizer(False)
        i = ['test-2', 'test!2', 'test@2', 'test#2', 'test$2',
             'test&2', 'test*2', 'test(2)', 'test)2', 'test[2]', 'test]2',
             'test{2}', 'test}2', 'test|2', 'test\\2', 'test^2', 'test~2'
             'test"2', 'test\'2', 'test`2', 'test<2', 'test>2', 'test=2',
             'test,2', 'test;2', 'test:2', 'test?2', 'test/2']
        with self.assertRaises(ValueError):
            s._sanitize_colnames(i)

        i = ['test', 'test_1', 'test_1_2', 'test_1_2', 'Test', 'Test_1']
        self.assertEqual(s._sanitize_colnames(i), [k.lower() for k in i])

    def test_sanitize_dict(self):
        s = _Sanitizer(False)
        d = {'test-2': 1, 'test!2': 2, 'test@2': 3, 'test#2': 4, 'test$2': 5}
        with self.assertRaises(ValueError):
            s._sanitize_colnames(d)

        d = {'test': 1, 'test_1': 2, 'tesT_1_2': 3, 'test_1_2': 4}
        sanit = s._sanitize_colnames(d)
        self.assertEqual(sanit, {k.lower(): v for k, v in d.items()})

    def test_sanitize_protected(self):
        for k in ['__id__', '__b32__testing']:
            with self.assertRaisesRegex(ValueError, 'protected'):
                s = _Sanitizer(False)
                s._sanitize_colnames(k)


class TestSanitizeColnamesB32(TestCaseWithNumpyCompare):
    def test_sanitize_string(self):
        s = _Sanitizer(True)
        for k, e in [('test-2', '__b32__ORSXG5BNGI'),
                     ('test!2', '__b32__ORSXG5BBGI'),
                     ('test@2', '__b32__ORSXG5CAGI'),
                     ('test#2', '__b32__ORSXG5BDGI'),
                     ('test$2', '__b32__ORSXG5BEGI')]:
            self.assertEqual(s._sanitize_colnames(k), e)

        for i in ['test', 'test_1', 'test_1_2', 'test_1_2', 'Test', 'Test_1']:
            self.assertEqual(s._sanitize_colnames(i), i.lower())

    def test_sanitize_list(self):
        s = _Sanitizer(True)
        names = ['test-2', 'test!2', 'test@2', 'test#2', 'test$2']
        v = ['__b32__ORSXG5BNGI', '__b32__ORSXG5BBGI', '__b32__ORSXG5CAGI',
             '__b32__ORSXG5BDGI', '__b32__ORSXG5BEGI']
        self.assertEqual(s._sanitize_colnames(names), v)

        i = ['test', 'test_1', 'test_1_2', 'test_1_2', 'Test', 'Test_1']
        self.assertEqual(s._sanitize_colnames(i), [k.lower() for k in i])

    def test_sanitize_dict(self):
        s = _Sanitizer(True)
        d = {'test-2': '__b32__ORSXG5BNGI',
             'test!2': '__b32__ORSXG5BBGI',
             'test@2': '__b32__ORSXG5CAGI',
             'test#2': '__b32__ORSXG5BDGI',
             'test$2': '__b32__ORSXG5BEGI'}
        self.assertEqual(s._sanitize_colnames(d), {v: v for v in d.values()})

        d = {'test': 1, 'test_1': 2, 'tesT_1_2': 3, 'test_1_2': 4}
        sanit = s._sanitize_colnames(d)
        self.assertEqual(sanit, {k.lower(): v for k, v in d.items()})

    def test_first_digit_error(self):
        s = _Sanitizer(False)
        with self.assertRaisesRegex(ValueError, 'Invalid'):
            s._sanitize_colnames('2test')

    def test_b32_padding(self):
        s = _Sanitizer(True)
        for i in range(64):
            bs = '-'+'a'*i
            encoded = s._encode_b32(bs)
            decoded = s._decode_b32(encoded)
            self.assertEqual(bs, decoded)


class TestSanitizeGetColumnName(TestCaseWithNumpyCompare):
    def test_get_column_name(self):
        s = _Sanitizer(False)
        self.assertEqual(s._get_column_name('table', 'test1'), 'test1')
        self.assertEqual(s._get_column_name('table','test2'), 'test2')

    def test_get_column_name_invalid(self):
        s = _Sanitizer(False)
        with self.assertRaisesRegex(ValueError, 'Invalid'):
            s._get_column_name('table', 'test-2')
        with self.assertRaisesRegex(ValueError, 'protected'):
            s._get_column_name('table', '__b32__ORSXG5BNGI')

    def test_get_column_name_not_in_database(self):
        s = _Sanitizer(False)
        with self.assertRaisesRegex(KeyError, 'not found'):
            s._get_column_name('table', 'test3')

    def test_get_column_name_b32(self):
        s = _Sanitizer(True)
        self.assertEqual(s._get_column_name('table', 'test1'), 'test1')
        self.assertEqual(s._get_column_name('table', 'test2'), 'test2')
        self.assertEqual(s._get_column_name('table', 'test-2'), '__b32__ORSXG5BNGI')

    def test_get_column_name_b32_not_in_database(self):
        s = _Sanitizer(True)
        with self.assertRaisesRegex(KeyError, 'not found'):
            s._get_column_name('table', 'test!2')

    def test_get_column_name_id(self):
        s = _Sanitizer(False)
        self.assertEqual(s._get_column_name('table', '__id__'), '__id__')
        self.assertEqual(s._get_column_name('table', '__ID__'), '__id__')
