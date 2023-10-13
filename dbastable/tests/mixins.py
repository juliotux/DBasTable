import unittest
import numpy as np


class TestCaseWithNumpyCompare(unittest.TestCase):
    def assertEqualArray(self, *args):
        return np.testing.assert_array_equal(*args)

    def assertAlmostEqualArray(self, *args):
        return np.testing.assert_array_almost_equal(*args)
