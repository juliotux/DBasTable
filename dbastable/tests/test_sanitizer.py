# Licensed under a 3-clause BSD style license - see LICENSE.rst
# flake8: noqa: F403, F405

from dbastable._sanitizer import _SanitizerMixin
from dbastable.tests.mixins import TestCaseWithNumpyCompare


class _Sanitizer(_SanitizerMixin):
    def __init__(self, allow_b32_colnames=False):
        self._allow_b32_colnames = allow_b32_colnames


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
        sanit = s._sanitize_colnames(i)
        self.assertEqual(s._sanitize_colnames(i), [k.lower() for k in i])

    def test_sanitize_dict(self):
        s = _Sanitizer(False)
        d = {'test-2': 1, 'test!2': 2, 'test@2': 3, 'test#2': 4, 'test$2': 5}
        with self.assertRaises(ValueError):
            s._sanitize_colnames(d)

        d = {'test': 1, 'test_1': 2, 'tesT_1_2': 3, 'test_1_2': 4}
        sanit = s._sanitize_colnames(d)
        self.assertEqual(sanit, {k.lower(): v for k, v in d.items()})


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
        l = ['test-2', 'test!2', 'test@2', 'test#2', 'test$2']
        v = ['__b32__ORSXG5BNGI', '__b32__ORSXG5BBGI', '__b32__ORSXG5CAGI',
             '__b32__ORSXG5BDGI', '__b32__ORSXG5BEGI']
        self.assertEqual(s._sanitize_colnames(l), v)

        i = ['test', 'test_1', 'test_1_2', 'test_1_2', 'Test', 'Test_1']
        sanit = s._sanitize_colnames(i)
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
