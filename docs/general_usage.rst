General Usage Guide
===================

`dbastable` intends to create and manipulate tables in SQLite databases in the same way you interact with `~astropy.table.Table` from `~astropy` package.

But, as an SQLite database can contain numerous tables, you need first to create an `~dbastable.SQLDatabase` object, which will be the entry point to the database. This database can be created in memory or on disk. If you pass a filename to the `~dbastable.SQLDatabase` constructor, the database will be created on disk. If you pass ``':memory:'`` as filename, the database will be created in memory.

.. code-block:: python

    >>> from dbastable import SQLDatabase
    >>> # this will create a database on disk
    >>> db = SQLDatabase('mydatabase.db')
    >>> # this will create a database in memory
    >>> db = SQLDatabase(':memory:')

Another useful argument is the ``autocommit``, which enable `~sqlite3` to commit changes automatically after each command. If you set it to ``False``, you will need to call ``commit()`` method to commit changes to the database. While ``autocommit`` reduces the need of manual method calls, it can reduce the performance of the database if a lot of small operations are done.

Data Creation, Manipulation and Deletion
----------------------------------------

Creating Tables
^^^^^^^^^^^^^^^

Once you have a database, you can create a table in it. The creation is performed by the `SQLDatabase.add_table` method. This method takes a table name as the main argument.

.. code-block:: python

    >>> # this creates a table called 'table_1' in the database
    >>> db.add_table('table_1')
    >>> # to access the created table, you can use the '[]' operator
    >>> db['table_1']
    SQLTable 'table_1' in database ':memory:':(0 columns x 0 rows)
    <No columns>

You can also create a table with a list of columns and/or data. The ``columns`` can be created as a list of strings, which are the column names. The ``data`` is a list of tuples, where each tuple represents a row of data. The rows must be ordered in the same way as the columns.

.. code-block:: python

    >>> # this creates a table called 'table_2' with 2 columns and 2 rows
    >>> db.add_table('table_with_data', columns=['name', 'birth'],
    ...              data=[('Someone', 2001), ('No one', 2002)])
    >>> db['table_with_data']
    SQLTable 'table_with_data' in database ':memory:':(2 columns x 2 rows)
      name  birth
    ------- -----
    Someone  2001
     No one  2002

.. code-block:: python

    >>> # this creates a table called 'table_with_columns' with 2 columns but not data
    >>> db.add_table('table_with_columns', columns=['name', 'birth'])
    >>> db['table_with_columns']
    SQLTable 'table_with_columns' in database ':memory:':(2 columns x 0 rows)
    name birth
    ---- -----

Adding Columns and Data
^^^^^^^^^^^^^^^^^^^^^^^

To add columns to your table, there is a method called `SQLDatabase.add_column`. The main arguments are the table name and the column name. You can also pass a ``data`` argument, which is a list of data to be added to the column. The data must be ordered in the same way as the rows of the already existing table. If no data is passed, the column will be created with ``None`` values.

.. code-block:: python

    >>> # column 'email' will be filled with None
    >>> db.add_column('table_with_data', 'email')
    >>> db.add_column('table_with_data', 'age', data=[22, 21])
    >>> db['table_with_data']
    SQLTable 'table_with_data' in database ':memory:':(4 columns x 2 rows)
      name  birth email age
    ------- ----- ----- ---
    Someone  2001  None  22
     No one  2002  None  21

You can also create new columns using ``__setitem__`` features.

.. code-block:: python

    >>> # create a new column 'email' with some data
    >>> db['table_with_columns']['email'] = ['test@test.org']
    >>> db['table_with_columns']
    SQLTable 'table_with_columns' in database ':memory:':(3 columns x 1 rows)
    name birth     email
    ---- ----- -------------
    None  None test@test.org

Adding Rows to the Table
^^^^^^^^^^^^^^^^^^^^^^^^

To add rows to the table, you can use the `SQLDatabase.add_rows` method. A ``table`` name must be passed as the first argument. The ``data`` fo the rows can be one of two types:

* A `list` of `tuple`, where each tuple represents a row of data. The rows must be ordered in the same way as the columns.

    .. code-block:: python

        >>> # this will add 2 rows to the table using a list of tuples
        >>> db.add_rows('table_with_data', [('Someone else', 2003, 'test@yes.no', 20),
        ...                                 ('Another one', 2004, 'test@no.yes', 19)])
        >>> db['table_with_data']
        SQLTable 'table_with_data' in database ':memory:':(4 columns x 4 rows)
            name     birth    email    age
        ------------ ----- ----------- ---
             Someone  2001        None  22
              No one  2002        None  21
        Someone else  2003 test@yes.no  20
         Another one  2004 test@no.yes  19

* A `dict`, where each key represents the data of a columns. All the elements must be scalars or 1d-lists with the same length.

    .. code-block:: python

        >>> # this will add 2 rows to the table using a list of tuples
        >>> db.add_rows('table_with_data', {'name': ['Mr. Mr', 'Mrs. Mrs'],
        ...                                 'birth': [1990, 1991],
        ...                                 'email': [None, 'test@test.test'],
        ...                                 'age': [30, 29]})
        >>> db['table_with_data']
        SQLTable 'table_with_data' in database ':memory:':(4 columns x 6 rows)
            name     birth     email      age
        ------------ ----- -------------- ---
             Someone  2001           None  22
              No one  2002           None  21
        Someone else  2003    test@yes.no  20
         Another one  2004    test@no.yes  19
              Mr. Mr  1990           None  30
            Mrs. Mrs  1991 test@test.test  29

When adding a row with a dict, you can optionally choose to add the missing columns to the table. This is done by passing ``add_columns=True`` to the `SQLDatabase.add_rows` method. The missing columns will be created with ``None`` values.

.. code-block:: python

    >>> db.add_rows('table_with_data', {'name': 'From Future', 'birth': 3000,
    ...                                 'flying_cars': 2}, add_columns=True)
    >>> db['table_with_data']
    SQLTable 'table_with_data' in database ':memory:':(5 columns x 7 rows)
        name     birth     email       age flying_cars
    ------------ ----- -------------- ---- -----------
         Someone  2001           None   22        None
          No one  2002           None   21        None
    Someone else  2003    test@yes.no   20        None
     Another one  2004    test@no.yes   19        None
          Mr. Mr  1990           None   30        None
        Mrs. Mrs  1991 test@test.test   29        None
     From Future  3000           None None           2

Acessing the Data
^^^^^^^^^^^^^^^^^

The access of data wase designed to use as mutch as possible the ``[]`` (`__getitem__`) operator. But, as SQLite interacts with a very different way with the data, we developed a series of :doc:`viewer class </viewer_classes>` that will interface the queries in the database. These classes are `~dbastable.SQLTable`, `~dbastable.SQLColumn` and `~dbastable.SQLRow`. These three viewer classes are also returned by the `SQLDatabase.get_table`, `SQLDatabase.get_column` and `SQLDatabase.get_row` methods.

When working with the ``[]`` operator, it's mandatory that the first element of the item to get is a table name, just like we did above to show the results. But you can access a column, a row or a cell with the same way.

To get a table:

.. code-block:: python

    >>> # get the table
    >>> table = db['table_with_data']
    >>> print(table)  # doctest: +ELLIPSIS
    SQLTable 'table_with_data' in database ':memory:':(5 columns x 7 rows)
    ...

To work with a column of a table:

.. code-block:: python

    >>> col = db['table_with_data', 'name']
    >>> print(col)
    SQLColumn 'name' in table 'table_with_data' (7 rows)
    >>> print(col.values)
    ['Someone', 'No one', 'Someone else', 'Another one', 'Mr. Mr', 'Mrs. Mrs', 'From Future']

.. code-block:: python

    >>> # or, you can get it from the SQLTable directly
    >>> col = table['name']
    >>> print(col)
    SQLColumn 'name' in table 'table_with_data' (7 rows)
    >>> print(col.values)
    ['Someone', 'No one', 'Someone else', 'Another one', 'Mr. Mr', 'Mrs. Mrs', 'From Future']

To work with a row of a table:

.. code-block:: python

    >>> row = db['table_with_data', 0]
    >>> print(row)
    SQLRow 0 in table 'table_with_data' {'name': 'Someone', 'birth': 2001, 'email': None, 'age': 22, 'flying_cars': None}
    >>> print(row.values)
    ('Someone', 2001, None, 22, None)

.. code-block:: python

    >>> # or, you can get it from the SQLTable directly
    >>> row = table[3]
    >>> print(row)
    SQLRow 3 in table 'table_with_data' {'name': 'Another one', 'birth': 2004, 'email': 'test@no.yes', 'age': 19, 'flying_cars': None}

When the depth level of the `_getitem__` reaches a cell, only the value is returned and not a full new viewer object. So, when you call for a row of a column or a column of a row, you will get the value of the cell.

.. code-block:: python

    >>> print(db['table_with_data', 0, 'name'])
    Someone
    >>> print(db['table_with_data', 'birth', 2])
    2003
    >>> # Also, you can operate rows as slices!
    >>> print(db['table_with_data', 'name', 1:3])
    ['No one', 'Someone else']

Changing the Data
^^^^^^^^^^^^^^^^^

In the same way, you can use the ``[]`` operator (`__setitem___`) to set data for cells, entire columns or entire rows. This operator, as mentioned earlier, can also be used to create new columns. It will perform the data setting by calling `SQLDatabase.set_item`, `SQLDatabase.set_column` or `SQLDatabase.set_row` methods.

.. code-block:: python

    >>> # everyone has a flying car now!
    >>> db['table_with_data', 'flying_cars'] = [2, 1, 3, 4, 1, 2, 6]
    >>> # Someone else was born in 1995
    >>> db['table_with_data', 2] = ('Someone else', 1995, 'someone@noplace.org', 28, 1)
    >>> # Mr. Mr now has an email
    >>> db['table_with_data', 4, 'email'] = 'mistermister@nomail.com'
    >>> print(db['table_with_data'])
    SQLTable 'table_with_data' in database ':memory:':(5 columns x 7 rows)
        name     birth          email          age  flying_cars
    ------------ ----- ----------------------- ---- -----------
         Someone  2001                    None   22           2
          No one  2002                    None   21           1
    Someone else  1995     someone@noplace.org   28           1
     Another one  2004             test@no.yes   19           4
          Mr. Mr  1990 mistermister@nomail.com   30           1
        Mrs. Mrs  1991          test@test.test   29           2
     From Future  3000                    None None           6

It's not possible to change the name of a column or a table. If you need to do that, you will need to create a new column or table with the new name and copy the data from the old one. Also, it's not possible to change all the data of a table at once. If you need to do that, you will need to drop the table and create a new one with the same name.

Deleting The Data
^^^^^^^^^^^^^^^^^

You can delete (drop) a table, delete columns or rows. This is done by calling the `SQLDatabase.drop_table`, `SQLDatabase.delete_column` or `SQLDatabase.delete_row` methods. The `del` methods where not implemented due to data integrity concerns. So, by calling these methods directly you are more aware of what you are doing.

.. code-block:: python

    >>> # we don't need the table_with_columns anymore
    >>> db.drop_table('table_with_columns')
    >>> # I don't care if they have a flying car or not
    >>> db.delete_column('table_with_data', 'flying_cars')
    >>> # From Future returned to future
    >>> db.delete_row('table_with_data', 6)
    >>> print(db['table_with_data'])
    SQLTable 'table_with_data' in database ':memory:':(4 columns x 6 rows)
        name     birth          email          age
    ------------ ----- ----------------------- ---
         Someone  2001                    None  22
          No one  2002                    None  21
    Someone else  1995     someone@noplace.org  28
     Another one  2004             test@no.yes  19
          Mr. Mr  1990 mistermister@nomail.com  30
        Mrs. Mrs  1991          test@test.test  29

Additional Features and Properties
----------------------------------

The `~dbastable.SQLDatabase` class has some additional features and properties that can be useful. You can interact with the table in a more SQL-like way, but without the need to write SQL commands. You can also get the list of tables in the database and the list of columns in a table, or the lenght of a table.

Properties
^^^^^^^^^^

The two main properties that you may use to interact with the database are `SQLDatabase.table_names` and `SQLDatabase.column_names`.

To get the list of tables in the database, you can use the `SQLDatabase.table_names` property. This property returns a list of strings, where each string is a table name. I the first time you call it, it will query the database to get the list of tables, and them, build a internal cache with the names. The next time you call it, it will use the cache instead of querying the database again. Also, modifying the table also leads to changes in the cache. *Because of this, do not change the database externally while the db is opened here!* This reduces the need of querying the database every time you need the list of tables, speeding up a lot the process.

.. code-block:: python

    >>> # let's add some more tables to get a list of tables
    >>> db.add_table('table_2')
    >>> db.add_table('table_3')
    >>> # get the list of tables in the database
    >>> db.table_names
    ['table_1', 'table_with_data', 'table_2', 'table_3']

A similar method is used to get the list of columns in a table. The `SQLDatabase.column_names` method receives a table name and return the list of the columns in that table. It also caches the names to speed up the process, so *the same warning mentioned earlier is also valid here*.

.. code-block:: python

    >>> # get the list of columns in a table
    >>> db.column_names('table_with_data')
    ['name', 'birth', 'email', 'age']

SQL Methods
^^^^^^^^^^^

There are two high level methods that you may want to use: `SQLDatabase.select` and `SQLDatabase.count`.

select
""""""

`SQLDatabase.select` perform the ``SELECT`` operation inside the database. However, do not need to write the command by yourself. You pass a bunch of pre-defined arguments to the method and it will build the command for you. The main arguments are:

* ``table``: The name of the table to be selected.
* ``columns``: A list of columns to be selected. If not passed, all the columns will be selected.
* ``where``: Conditions to select a row or not.
* ``order``: Columns to be used to sort the rows.
* ``limit`` and ``offset``: Limits the number of rows to be selected.

Only values will be returned, so do not expect a full table. The result will be a list of tuple, with each tuple being a row of data.

Look that not all features of SQL ``SELECT`` can be used. This is intentional to keep the method simple. If you need more complex queries, you need to write the commands by yourself and use the proper functions described below.

.. code-block:: python

    >>> # select all the columns from the table
    >>> db.select('table_with_data', columns=['name', 'birth', 'age'],
    ...           order=['birth'], limit=4, offset=1)
    [('Mrs. Mrs', 1991, 29), ('Someone else', 1995, 28),
     ('Someone', 2001, 22), ('No one', 2002, 21)]

:doc:`Where Statemens </where_statements>` can be used. A better destription is found in the link, but here is a quick example:

.. code-block:: python

    >>> from dbastable import Where
    >>> # select all the columns from the table where age is greater than 25
    >>> db.select('table_with_data', columns=['name', 'birth', 'age'],
    ...           where=Where('age', '>', 25), order=['age'])
    [('Someone else', 1995, 28), ('Mrs. Mrs', 1991, 29), ('Mr. Mr', 1990, 30)]

count
"""""

`SQLDatabase.count` perform the ``COUNT`` operation inside the database. In addition to the table name, it just receives a ``where`` argument, which is a :doc:`Where Statement </where_statements>`.

.. code-block:: python

    >>> # count the number of rows in the table
    >>> db.count('table_with_data')
    6
    >>> # count the number of rows where age is greater than 25
    >>> db.count('table_with_data', where=Where('age', '>', 25))
    3

execute and executemany
"""""""""""""""""""""""

It you have an SQL command that you manually wrote and want to execute, ou can use the `SQLDatabase.execute` and `SQLDatabase.executemany` method. They are the lower level methods called by all other methods and wraps `~sqlite3.Cursor.execute` and `~sqlite3.Cursor.executemany` methods. The first one is used to execute a single command, while the second one is used to execute a command multiple times, with different parameters. Both accept the `arguments` parameter if you want to use the ``?`` `placeholder <https://docs.python.org/3/library/sqlite3.html#sqlite3-placeholders>`_ in the command and avoid `SQL injection hacks <https://en.wikipedia.org/wiki/SQL_injection>`_. `Learn more with Little Bob <https://xkcd.com/327/>`_.

The direct use of these methods may not be needed, since we already handle almost all operations you may need. But, if you need a very specific case, like `SUM` columns, you can use them.

.. code-block:: python

    >>> # sum the age of all people in the table
    >>> db.execute('SELECT SUM(age) FROM table_with_data')
    [(149,)]
    >>> # sum the age of all people in the table where age is greater than 25
    >>> db.execute('SELECT SUM(age) FROM table_with_data WHERE age > ?',
    ...            arguments=[25])
    [(87,)]

commit
""""""

The `SQLDatabase.commit` method is used to apply changes to the database. When a database is changed, `~sqlite3` do not change the original database as default. It save the changes to a separated diff file and only merge it when the ``commit`` command is called. It's called automatically if ``autocommit`` is set to ``True``. If not, you will need to call it manually.
