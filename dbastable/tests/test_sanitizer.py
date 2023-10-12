# Licensed under a 3-clause BSD style license - see LICENSE.rst
# flake8: noqa: F403, F405

from dbastable._parse_tools import _sanitize_colnames
from .mixins import TestCaseWithNumpyCompare


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
