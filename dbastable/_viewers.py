import numpy as np

from ._def import _ID_KEY


class SQLTable:
    """Handle an SQL table operations interfacing with the DB.

    Parameters
    ----------
    db : SQLDatabase
        The parent database object.
    name : str
        The name of the table in the database.
    """

    def __init__(self, db, name):
        self._db = db
        self._name = name

    @property
    def name(self):
        """Get the name of the table."""
        return self._name

    @property
    def db(self):
        """Get the database name."""
        return self._db._db

    @property
    def column_names(self):
        """Get the column names of the current table."""
        names = self._db.column_names(self._name)
        return names

    @property
    def values(self):
        """Get the values of the current table."""
        return self.select()

    def select(self, **kwargs):
        """Select rows from the table. See `~dbastable.SQLDatabase.select`.

        Parameters
        ----------
        columns : list (optional)
            List of columns to select. If None, select all columns.
        where : dict (optional)
            Dictionary of conditions to select rows. Keys are column names,
            values are values to compare. If it is a dict of values, all rows
            equal to the values will be selected. If it is a dict of
            `~dbastable.where.Where` objects, the conditions will be combined
            with the AND operator. If None, all rows are selected.
        order : str (optional)
            Column name to order by.
        limit : int (optional)
            Number of rows to select.
        offset : int (optional)
            Number of rows to skip before selecting.

        Returns
        -------
        res : list
            List of tuples with the selected rows. Each row values will be
            returned in a tuple in the same order as the columns.
        """
        return self._db.select(self._name, **kwargs)

    def as_table(self):
        """Return the current table as an `~astropy.table.Table` object."""
        from astropy.table import Table

        if len(self) == 0:
            return Table(names=self.column_names)

        return Table(rows=self.values,
                     names=self.column_names)

    def add_column(self, name, data=None):
        """Add a column to the table. See `~dbastable.SQLDatabase.add_column`.

        Parameters
        ----------
        name : str
            Column name.
        data : list (optional)
        """
        self._db.add_column(self._name, name, data=data)

    def add_rows(self, data, add_columns=False):
        """Add a row to the table. See `~dbastable.SQLDatabase.add_rows`.

        Parameters
        ----------
        data : dict, list or `~numpy.ndarray`
            Data to add to the table. If dict, keys are column names,
            if list, the order of the values is the same as the order of
            the column names. If `~numpy.ndarray`, dtype names are interpreted
            as column names.
        add_columns : bool (optional)
            If True, add missing columns to the table.
        """
        self._db.add_rows(self._name, data, add_columns=add_columns)

    def get_column(self, column):
        """Get a given column from the table.
        See `~dbastable.SQLDatabase.get_column`.

        Parameters
        ----------
        column : str
            Column name.

        Returns
        -------
        res : `~dbastable.SQLColumn`
            Column viewer object.
        """
        return self._db.get_column(self._name, column)

    def get_row(self, row):
        """Get a given row from the table.
        See `~dbastable.SQLDatabase.get_row`.

        Parameters
        ----------
        row : int
            Row index.

        Returns
        -------
        res : `~dbastable.SQLRow`
            Row viewer object.
        """
        return self._db.get_row(self._name, row)

    def set_column(self, column, data):
        """Set a given column in the table.
        See `~dbastable.SQLDatabase.set_column`.

        Parameters
        ----------
        column : str
            Column name.
        data : list
            List of values to set.
        """
        self._db.set_column(self._name, column, data)

    def set_row(self, row, data):
        """Set a given row in the table.
        See `~dbastable.SQLDatabase.set_row`.

        Parameters
        ----------
        row : int
            Row index.
        data : dict, list or `~numpy.ndarray`
            Data to add to the table. If dict, keys are column names,
            if list, the order of the values is the same as the order of
            the column names. If `~numpy.ndarray`, dtype names are interpreted
            as column names.
        """
        self._db.set_row(self._name, row, data)

    def delete_column(self, column):
        """Delete a given column from the table.
        See `~dbastable.SQLDatabase.delete_column`.

        Parameters
        ----------
        column : str
            Column name.
        """
        self._db.delete_column(self._name, column)

    def delete_row(self, row):
        """Delete all rows from the table.
        See `~dbastable.SQLDatabase.delete_row`.

        Parameters
        ----------
        row : int
            Row index.
        """
        self._db.delete_row(self._name, row)

    def index_of(self, where):
        """Get the index of the rows that match the given condition.
        See `~dbastable.SQLDatabase.index_of`.

        Parameters
        ----------
        where : dict
            Dictionary of conditions to select rows. Keys are column names,
            values are values to compare. If it is a dict of values, all rows
            equal to the values will be selected. If it is a dict of
            `~dbastable.where.Where` objects, the conditions will be combined
            with the AND operator. If None, all rows are selected.

        Returns
        -------
        res : list
            List of row indexes.
        """
        return self._db.index_of(self._name, where)

    def _resolve_tuple(self, key):
        """Resolve how tuples keys are handled."""
        col, row = key
        _tuple_err = """Tuple items must be in the format table[col, row] or
        table[row, col].
        """

        if not isinstance(col, str):
            # Try inverting
            col, row = row, col

        if not isinstance(col, str):
            raise KeyError(_tuple_err)

        if not isinstance(row, (int, slice, list, np.ndarray)):
            raise KeyError(_tuple_err)

        return col, row

    def __getitem__(self, key):
        """Get a row or a column from the table."""
        if isinstance(key, (int, np.int_)):
            return self.get_row(key)
        if isinstance(key, (str, np.str_)):
            return self.get_column(key)
        if isinstance(key, tuple):
            if len(key) not in (1, 2):
                raise KeyError(f'{key}')
            if len(key) == 1:
                return self[key[0]]
            col, row = self._resolve_tuple(key)
            return self[col][row]
        raise KeyError(f'{key}')

    def __setitem__(self, key, value):
        """Set a row or a column in the table."""
        if isinstance(key, int):
            self.set_row(key, value)
        elif isinstance(key, str):
            self.set_column(key, value)
        elif isinstance(key, tuple):
            if len(key) not in (1, 2):
                raise KeyError(f'{key}')
            if len(key) == 1:
                self[key[0]] = value
            else:
                col, row = self._resolve_tuple(key)
                self[col][row] = value
        else:
            raise KeyError(f'{key}')

    def __len__(self):
        """Get the number of rows in the table."""
        return self._db.count(self._name)

    def __contains__(self, item):
        """Check if a given column is in the table."""
        return item in self.column_names

    def __iter__(self):
        """Iterate over the rows of the table."""
        for i in self.select():
            yield i

    def __repr__(self):
        """Get a string representation of the table."""
        s = f"{self.__class__.__name__} '{self.name}'"
        s += f" in database '{self.db}':"
        s += f"({len(self.column_names)} columns x {len(self)} rows)\n"
        s += '\n'.join(self.as_table().__repr__().split('\n')[1:])
        return s


class SQLColumn:
    """Handle an SQL column operations interfacing with the DB."""

    def __init__(self, db, table, name):
        """Initialize the column.

        Parameters
        ----------
        db : SQLDatabase
            The parent database object.
        table : str
            The name of the table in the database.
        name : str
            The column name in the table.
        """
        self._db = db
        self._table = table
        self._name = name

    @property
    def name(self):
        """Get the name of the column."""
        return self._name

    @property
    def values(self):
        """Get the values of the current column."""
        vals = self._db.select(self._table, columns=[self._name])
        return [i[0] for i in vals]

    @property
    def table(self):
        """Get the table name."""
        return self._table

    def __getitem__(self, key):
        """Get a row from the column."""
        if isinstance(key, (int, np.int_, slice)):
            return self.values[key]
        if isinstance(key, (list, np.ndarray)):
            v = self.values
            return [v[i] for i in key]
        raise IndexError(f'{key}')

    def __setitem__(self, key, value):
        """Set a row in the column."""
        if isinstance(key, (int, np.int_)):
            self._db.set_item(self._table, self._name, key, value)
        elif isinstance(key, (slice, list, np.ndarray)):
            v = np.array(self.values)
            v[key] = value
            self._db.set_column(self._table, self._name, v)
        else:
            raise IndexError(f'{key}')

    def __len__(self):
        """Get the number of rows in the column."""
        return len(self.values)

    def __iter__(self):
        """Iterate over the column."""
        for i in self.values:
            yield i

    def __contains__(self, item):
        """Check if the column contains a given value."""
        return item in self.values

    def __repr__(self):
        """Get a string representation of the column."""
        s = f"{self.__class__.__name__} {self._name} in table '{self._table}'"
        s += f" ({len(self)} rows)"
        return s


class SQLRow:
    """Handle and SQL table row interfacing with the DB."""

    def __init__(self, db, table, row):
        """Initialize the row.

        Parameters
        ----------
        db : SQLDatabase
            The parent database object.
        table : str
            The name of the table in the database.
        row : int
            The row index in the table.
        """
        self._db = db
        self._table = table
        self._row = row

    @property
    def column_names(self):
        """Get the column names of the current table."""
        names = self._db.column_names(self._table)
        return names

    @property
    def table(self):
        """Get the table name."""
        return self._table

    @property
    def values(self):
        """Get the values of the current row."""
        return self._db.select(self._table, where={_ID_KEY: self.index+1})[0]

    @property
    def index(self):
        """Get the index of the current row."""
        return self._row

    @property
    def keys(self):
        """Get the keys of the current row."""
        return self.column_names

    @property
    def items(self):
        """Get the items of the current row."""
        return zip(self.column_names, self.values)

    def as_dict(self):
        """Get the row as a dict."""
        return dict(self.items)

    def __getitem__(self, key):
        """Get a column from the row."""
        if isinstance(key, (str, np.str_)):
            column = key
            try:
                return self._db.get_item(self._table, column, self._row)
            except ValueError:
                raise KeyError(f'{key}')
        if isinstance(key, (int, np.int_)):
            return self.values[key]
        raise KeyError(f'{key}')

    def __setitem__(self, key, value):
        """Set a column in the row."""
        if not isinstance(key, (str, np.str_)):
            raise KeyError(f'{key}')

        column = key = key.lower()
        if key not in self.column_names:
            raise KeyError(f'{key}')
        self._db.set_item(self._table, column, self.index, value)

    def __iter__(self):
        """Iterate over the row."""
        for i in self.values:
            yield i

    def __contains__(self, item):
        """Check if the row contains a given value."""
        return item in self.values

    def __repr__(self):
        """Get a string representation of the row."""
        s = f"{self.__class__.__name__} {self._row} in table '{self._table}' "
        s += self.as_dict().__repr__()
        return s
