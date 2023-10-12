import unittest
import numpy as np


class TestCaseWithNumpyCompare(unittest.TestCase):
    def assertEqualArray(self, *args):
        return np.testing.assert_array_equal(*args)
