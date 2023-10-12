import numpy as np


class _scalar_iterator:
    """Iterator for scalar values."""
    def __init__(self, value, length):
        self.value = value
        self.length = length

    def __iter__(self):
        for i in range(self.length):
            yield self.value

    def __getitem__(self, index):
        if index >= self.length or index < 0:
            raise IndexError
        return self.value

    def __len__(self):
        return self.length


class broadcast:
    """Broadcast values to the list size. Alternative to `~numpy.broadcast`."""

    def __init__(self, *args):
        """Initialize the broadcast object."""
        if len(args) == 0:
            raise ValueError("Empty broadcast")

        lengths = np.array([self._get_length(a) for a in args])
        if np.all(lengths == -1):
            self.length = 1
        else:
            self.length = [i for i in lengths if i >= 0]
            if len(set(self.length)) > 1:
                raise ValueError("All array arguments must have the same "
                                 "length.")
            self.length = self.length[0]
        self.args = [a if lengths[i] >= 0 else _scalar_iterator(a, self.length)
                     for i, a in enumerate(args)]

    def __iter__(self):
        """Return the iterator."""
        for i in zip(*self.iters):
            yield i

    @staticmethod
    def _get_length(value):
        """Get the length of iterable only values."""
        if value is None:
            return -1
        if np.isscalar(value):
            return -1
        return len(value)

    @property
    def iters(self):
        """Return the tuple containing the iterators."""
        return self.args

    def __len__(self):
        """Return the length of the broadcast."""
        return self.length
