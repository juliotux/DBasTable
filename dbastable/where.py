"""Classes to hande ``WHERE`` statements of `sqlite3`."""
import numpy as np


allowed_ops = ["=", "!=", ">", "<", ">=", "<=", "LIKE", "IN", "NOT IN", "IS",
               "IS NOT", "BETWEEN", "NOT BETWEEN"]


class Where:
    """Where statement generator for SQL queries.

    Parameters
    ----------
    column : str
        The column name.
    op : str
        The SQL operator. Must be one of:
        ``=, !=, >, <, >=, <=, LIKE, IN, NOT IN, IS, IS NOT, BETWEEN, NOT
        BETWEEN``.
    value : any
        The value to compare with the column. If the operator is ``IN`` or
        ``NOT IN``, the value must be a list of values. If the operator is
        ``BETWEEN`` or ``NOT BETWEEN``, the value must be a list of two values.
        For all other operators, the value must be a single value.
    """

    def __init__(self, column, op, value):
        self.column = column
        op = op.upper()
        if op not in allowed_ops:
            raise ValueError(f"Operator {op} not allowed. Supported are: "
                             f"{', '.join(allowed_ops)}.")
        self.op = op
        value = list(np.atleast_1d(value))
        if self.op in ['BETWEEN', 'NOT BETWEEN']:
            if len(value) != 2:
                raise ValueError(f'For {op} two values must be given.')
        elif self.op in ['IN', 'NOT IN']:
            if len(value) == 0:
                raise ValueError(f'For {op} at least one value must be given.')
        elif len(value) != 1:
            raise ValueError(f'For {op} just one value must be given')
        self.value = value

    @property
    def to_sql(self):
        """Generate a string for the where statement."""
        if self.op in ['BETWEEN', 'NOT BETWEEN']:
            return f"{self.column} {self.op} ? AND ?", self.value
        elif self.op in ['IN', 'NOT IN']:
            s = f"{self.column} {self.op} ({', '.join(['?']*len(self.value))})"
            return s, self.value
        return f"{self.column} {self.op} ?", self.value

    def __str__(self):
        if self.op in ['BEWEEN', 'NOT BETWEEN']:
            s = f"{self.column} {self.op} {self.value[0]} AND {self.value[1]}"
        else:
            s = f"{self.column} {self.op} {self.value}"
        return s

    def __repr__(self):
        s = f"{self.__class__.__name__}"
        s += f"(column={self.column}, op={self.op}, value={self.value})"
        return s


class _WhereParserMixin:
    """Mixin to handle the where statement parsing in the SQLDatabase."""

    def _parse_where(self, table, where):
        # None where have None statement and None arguments
        if where is None:
            return None, None

        args = []  # arguments to replace ?
        _where = []  # where statements with ?

        def _parse_token(key, value):
            # Parse a simgle token and return the where statement with argument
            # check if column exists
            col = self._get_column_name(table, key)
            # if the where is a _BaseWhere instance, just return the sql str
            if isinstance(value, Where):
                # use the parsed column name
                nw = Where(col, value.op, value.value)
                w, a = nw.to_sql
            # if is a value, assume it to be equal
            else:
                value = self._sanitize_value(value)
                w, a = Where(col, '=', value).to_sql

            _where.append(w)  # append the where statement
            args.extend(a)  # as a is returned as a list, add it

        if isinstance(where, Where):
            _parse_token(where.column, where)

        elif isinstance(where, dict):
            # no need for key sanitization. It goes into _parse_token
            for k, v in where.items():
                _parse_token(k, v)

        elif isinstance(where, (list, tuple)):
            for w in where:
                if not isinstance(w, Where):
                    raise TypeError('if where is a list, it must be a list '
                                    f'of Where instances. Not {type(w)}.')
                _parse_token(w.column, w)

        elif where is not None:
            raise TypeError(f'{type(where)} not supported for where.')

        return ' AND '.join(_where), [self._sanitize_value(a) for a in args]
