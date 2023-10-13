from dbastable._broadcaster import broadcast
from dbastable.tests.mixins import TestCaseWithNumpyCompare
import numpy as np


class Test_Broadcast(TestCaseWithNumpyCompare):
    def test_broadcast(self):
        a = np.arange(10)
        b = np.arange(1, 11)
        c = 2

        bc = broadcast(a, b, c)
        iterator = iter(bc)

        indx = 0
        for i in range(10):
            self.assertEqualArray(next(iterator), [a[indx], b[indx], c])
            indx += 1

        with self.assertRaises(StopIteration):
            next(iterator)

    def test_broadcast_empty(self):
        with self.assertRaises(ValueError):
            broadcast()

    def test_broadcast_wrong_shape(self):
        a = np.arange(10)
        b = np.arange(5)
        with self.assertRaises(ValueError):
            broadcast(a, b)

    def test_broadcast_only_scalars(self):
        a = 1
        b = 2
        c = 3
        bc = broadcast(a, b, c)
        for i in bc:
            self.assertEqualArray(i, [a, b, c])

    def test_broadcast_superpass_32_limit(self):
        arr = [np.arange(10)]*64
        bc = broadcast(*arr)
        self.assertEqualArray(len(bc), 10)
        it = iter(bc)
        for i in range(10):
            self.assertEqualArray(next(it), [i]*64)

    def test_broadcast_iters_only_scalars(self):
        bc = broadcast(1, 2, 3)
        self.assertEqualArray(bc.iters, [[1], [2], [3]])

    def test_broadcast_iters(self):
        bc = broadcast(np.arange(10), 3, 2)
        self.assertEqualArray(bc.iters, [np.arange(10), [3]*10, [2]*10])

    def test_broadcast_with_None(self):
        bc = broadcast(np.arange(10), None, 2)
        self.assertEqualArray(bc.iters, [np.arange(10), [None]*10, [2]*10])
