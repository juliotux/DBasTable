"""Classes to hande ``WHERE`` statements of `sqlite3`."""

import abc


class _BaseWhere(abc.ABC):
    def __init__(self, value):
        self.value = value

    @abc.abstractmethod
    def parse(self, colname):
        """Generate a string for the where statement."""
        pass

    def __repr__(self):
        return f"{self.__class__.__name__}({self.value!r})"


class Equal(_BaseWhere):
    """Class to handle the ``=`` operator."""

    def parse(self, colname):
        """Generate a string for the where statement."""
        return f"{colname} = {self.value}"


class NotEqual(_BaseWhere):
    """Class to handle the ``!=`` operator."""

    def parse(self, colname):
        """Generate a string for the where statement."""
        return f"{colname} != {self.value}"


class Greater(_BaseWhere):
    """Class to handle the ``>`` operator."""

    def parse(self, colname):
        """Generate a string for the where statement."""
        return f"{colname} > {self.value}"


class GreaterEqual(_BaseWhere):
    """Class to handle the ``>=`` operator."""

    def parse(self, colname):
        """Generate a string for the where statement."""
        return f"{colname} >= {self.value}"


class Less(_BaseWhere):
    """Class to handle the ``<`` operator."""

    def parse(self, colname):
        """Generate a string for the where statement."""
        return f"{colname} < {self.value}"


class LessEqual(_BaseWhere):
    """Class to handle the ``<=`` operator."""

    def parse(self, colname):
        """Generate a string for the where statement."""
        return f"{colname} <= {self.value}"


class Like(_BaseWhere):
    """Class to handle the ``LIKE`` operator."""

    def parse(self, colname):
        """Generate a string for the where statement."""
        return f"{colname} LIKE {self.value}"


class In(_BaseWhere):
    """Class to handle the ``IN`` operator."""

    def parse(self, colname):
        """Generate a string for the where statement."""
        return f"{colname} IN {self.value}"


class NotIn(_BaseWhere):
    """Class to handle the ``NOT IN`` operator."""

    def parse(self, colname):
        """Generate a string for the where statement."""
        return f"{colname} NOT IN {self.value}"


class Is(_BaseWhere):
    """Class to handle the ``IS`` operator."""

    def parse(self, colname):
        """Generate a string for the where statement."""
        return f"{colname} IS {self.value}"


class IsNot(_BaseWhere):
    """Class to handle the ``IS NOT`` operator."""

    def parse(self, colname):
        """Generate a string for the where statement."""
        return f"{colname} IS NOT {self.value}"


class Between(_BaseWhere):
    """Class to handle the ``BETWEEN`` operator."""

    def __init__(self, low, high):
        self.value = (low, high)

    def parse(self, colname):
        """Generate a string for the where statement."""
        return f"{colname} BETWEEN {self.value[0]} AND {self.value[1]}"


class NotBetween(_BaseWhere):
    """Class to handle the ``NOT BETWEEN`` operator."""

    def __init__(self, low, high):
        self.value = (low, high)

    def parse(self, colname):
        """Generate a string for the where statement."""
        return f"{colname} NOT BETWEEN {self.value[0]} AND {self.value[1]}"
