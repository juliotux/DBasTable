Viewer Classes
==============

The planned interaction with the database was only possible by the creation of 3 viewer classes. This classes act like a `view of Numpy arrays <https://numpy.org/doc/stable/reference/generated/numpy.ndarray.view.html>`_, but interfacing with all the SQL commands `~sqlite3` needs. So, they make possible the use of all `__getitem__` and `__setitem__` methods (``[]`` operator) inside a database.
`
These classes do not store any data itself. Instead, they just store references of rows, columns or tables to query and access the database. So, any changes in the data inside the database also affects the data returned by the viewer classes.

Also, it is not recommended to create them manually. Let the `~dbastable.SQLDatabase` create them for you.

`~dbastable.SQLTable`
---------------------

`~dbastable.SQLTable` is a class that can be used to view and manage a single table inside the database. It is return with ``[table]`` operator of `~dbastable.SQLDatabase` or with `SQLDatabase.get_table` method.

.. code-block:: python

    >>> from dbastable import SQLDatabase
    >>> db = SQLDatabase()
    >>> db.add_table('table1', columns=['name', 'age'],
    ...              data=[['John', 20], ['Mary', 30]])
    >>> table = db['table1']
    >>> print(table)
    SQLTable 'table1' in database 'None':(2 columns x 2 rows)
    name age
    ---- ---
    John  20
    Mary  30

It perform almost any important operation of `~dbastable.SQLDatabase`, but dismissing the ``table`` argument in all methods that methods. For example, ``add_rows``, ``delete_row``, ``add_column``, ``delete_column``, ``select``, ``index_of`` and more. More detais about these functions in the `~dbastable.SQLTable` API documentation.

One important thing to mention is how ``[]`` operator work in this class. It can be used to access a single row, column or cell inside the table. If a `int` number is used in the operator, it will return a `~dbastable.SQLRow` object. If a `str` is used, it will return a `~dbastable.SQLColumn` object. If a tuple of `int` and `str` is used, it will return a single cell value. No other combination is supported, so you cannot access multiple rows or columns at the same time.

.. code-block:: python

    >>> # int returns a row
    >>> print(table[0])
    SQLRow 0 in table 'table1' {'name': 'John', 'age': 20}
    >>> # str returns a column
    >>> print(table['name'])
    SQLColumn 'name' in table 'table1' (2 rows)
    >>> # tuple of int and str returns a cell
    >>> print(table[0, 'name'])
    John
    >>> print(table['name', 1])
    Mary


`~dbastable.SQLRow` and `~dbastable.SQLColumn`
----------------------------------------------

`~dbastable.SQLRow` and `~dbastable.SQLColumn` are classes that can be used to access and change data of a single row or column inside the database. They are able to query data from the database, change data of a single cell, but they don't inherit methods like ``add_rows`` or ``delete_row``. So you can't change the structure of the table using these classes. For this, you need to use `~dbastable.SQLTable`.

They are returned with ``[row]`` and ``[column]`` operators of `~dbastable.SQLTable` or with `SQLTable.get_row` and `SQLTable.get_column` methods.

.. code-block:: python

    >>> row = table[0]
    >>> print(row)
    SQLRow 0 in table 'table1' {'name': 'John', 'age': 20}
    >>> column = table['name']
    >>> print(column)
    SQLColumn 'name' in table 'table1' (2 rows)

.. code-block:: python

    >>> print(table.get_row(0))
    SQLRow 0 in table 'table1' {'name': 'John', 'age': 20}
    >>> print(table.get_column('name'))
    SQLColumn 'name' in table 'table1' (2 rows)

Also, they can be directly created with `~dbastable.SQLDatabase.get_row` and `~dbastable.SQLDatabase.get_column` methods.

.. code-block:: python

    >>> row = db.get_row('table1', 0)
    >>> print(row)
    SQLRow 0 in table 'table1' {'name': 'John', 'age': 20}
    >>> column = db.get_column('table1', 'name')
    >>> print(column)
    SQLColumn 'name' in table 'table1' (2 rows)

Both classes have a ``values`` property, which returns a list containing copies of the data from the given row or column. It is important to remember that the values returned are not linked anymore with the original database and any change in this list will not imply in changes in database.

.. code-block:: python

    >>> print(row.values)
    ('John', 20)
    >>> print(column.values)
    ['John', 'Mary']

Also, both methods are iterable using `for` loops. The iteration is always performed in the order of the table.

.. code-block:: python

    >>> for value in row:
    ...     print(value)
    John
    20
    >>> for value in column:
    ...     print(value)
    John
    Mary

The usage of `in` comparison statment is also possible. It will return ``True`` if the given value is in the row or column values.

.. code-block:: python

    >>> print('Mary' in row)
    False
    >>> print('Mary' in column)
    True

`~dbastable.SQLRow` Specifics
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

`~dbastable.SQLRow` have another special features that make it act like a `dict`. First of all, you can create a dictionary from it using ``as_dict`` method.

.. code-block:: python

    >>> print(row.as_dict())
    {'name': 'John', 'age': 20}

Also, it is iterable just like dicts using ``keys`` and ``items`` properties.

.. code-block:: python

    >>> for key in row.keys():
    ...     print(key)
    name
    age
    >>> for key, value in row.items():
    ...     print(key, value)
    name John
    age 20
