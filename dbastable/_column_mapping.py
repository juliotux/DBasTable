

class SQLColumnMap:
    """Map keywords to SQL columns."""

    def __init__(self, db, map_table, map_key, map_column):
        self.db = db
        self.map = db[map_table]
        self.key = map_key
        self.col = map_column

        self._clear_cache()

    def add_column(self, name):
        """Add a new column to the table."""
        name = name.lower()

        if name in self.keywords:
            raise ValueError(f'{name} already exists')

        i = len(self.keywords)+1
        col = f'col_{i}'
        while col in self.keywords:
            i += 1
            col = f'col_{i}'

        self.map.add_rows({self.key: name, self.col: col})
        self._clear_cache()
        return col

    def get_column_name(self, item, add_columns=False):
        """Get the column name for a given keyword."""
        if check_iterable(item):
            return [self.get_column_name(i) for i in item]

        item = item.lower()
        if item not in self.keywords:
            if add_columns:
                return self.add_column(item)
            raise KeyError(f'{item}')

        return self.columns[np.where(self.keywords == item)][0]

    def get_keyword(self, item):
        """Get the keyword for a given column."""
        if check_iterable(item):
            return [self.get_keyword(i) for i in item]

        item = item.lower()
        if item not in self.columns:
            raise KeyError(f'{item}')

        return self.keywords[np.where(self.columns == item)][0]

    def _clear_cache(self):
        self._columns = None
        self._keywords = None

    @property
    def columns(self):
        """Get the column names for the table."""
        if self._columns is None:
            self._columns = np.array(self.map.select(columns=[self.col]))
        return self._columns

    @property
    def keywords(self):
        """Get the keywords of the columns for the table."""
        if self._keywords is None:
            self._keywords = np.array(self.map.select(columns=[self.key]))
        return self._keywords

    def map_row(self, data, add_columns=False):
        """Map a row to the columns."""
        if isinstance(data, dict):
            d = {}
            for k, v in data.items():
                if k in self.keywords or add_columns:
                    d[self.get_column_name(k, add_columns=add_columns)] = v
            data = d
        elif not isinstance(data, list):
            raise ValueError('Only dict and list are supported')
        return data

    def parse_where(self, where):
        """Parse a where clause using column mappring."""
        if isinstance(where, dict):
            return {self.get_column_name(k): v for k, v in where.items()}
        raise TypeError('Only dict is supported')
