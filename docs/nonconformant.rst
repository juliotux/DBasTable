Non-Conformant Column Names
---------------------------

By default, any operation that interfaces with column names will only accept names that conform to the SQLite names standard. This means that column names must begin with a letter, and can only contain letters, numbers, and underscores. If you try to access a column that does not conform to this standard, you will get an error.

.. code-block:: python

    >>> from dbastable import SQLDatabase
    >>> db = SQLDatabase()
    >>> db.add_table('my_table')
    >>> db.add_column('my_table', '1test')
    ...
    ValueError: Invalid column name: 1test.
    >>> db.add_column('my_table', 'test column')
    ...
    ValueError: Invalid column name: test column.
    >>> db.add_column('my_table', 'test!with!exclamation!points')
    ...
    ValueError: Invalid column name: test!with!exclamation!points.

However, you can force the database to accept these non-conformant column names by setting the ``allow_b32_colnames`` parameter to ``True`` when you create the database. This will enable the non-conformant names to be encoded to `Base32 encoding <https://en.wikipedia.org/wiki/Base32>`_, and decoded back to their original names when you access them. This name encoding will be done by `base64.b32encode` and `base64.b32decode` from the Python standard library automatically.

When using this, the real column name will be ``__b32__<encoded_name>``, already striped by ``===`` padding added by the encoder. This will make the string unreadeble by humans, but it will be decoded back to the original name when you access it. For example, if you have a column named ``1test``, it will be encoded to ``__b32__GF2GK43U``, and if you have a column named ``test column``, it will be encoded to ``__b32__ORSXG5BAMNXWY5LNNY``. So, if you want to access the database outside of Python, it is recomended to *do not use this feature*. Also, everytime you use this database, remember to set the ``allow_b32_colnames`` parameter to ``True`` to properly access the column names.

If you are using `~dbastable.SQLDatabase`, the access of the columns will be fully transparent to the user and you will not need to worry about the encoding and decoding of the column names.

.. code-block:: python

    >>> db = SQLDatabase(allow_b32_colnames=True)
    >>> db.add_table('my_table')
    >>> db.add_column('my_table', '1test', data=[1, 2, 3])
    >>> db.add_column('my_table', 'test column', data=[1, 2, 3])
    >>> db.add_column('my_table', 'test!with!exclamation!points', data=[1, 2, 3])
    >>> db.column_names('my_table')
    ['1test', 'test column', 'test!with!exclamation!points']

If you want to access the original column names, you can use the ``do_not_decode`` parameter of `~dbastable.SQLDatabase.column_names` to get the encoded names.

.. code-block:: python

    >>> db.column_names('my_table', do_not_decode=True)
    ['__b32__GF2GK43U',
     '__b32__ORSXG5BAMNXWY5LNNY',
     '__b32__ORSXG5BBO5UXI2BBMV4GG3DBNVQXI2LPNYQXA33JNZ2HG']

If the column does not need to be encoded, it will not be encoded.

.. code-block:: python

    >>> db.add_column('my_table', 'test')
    >>> db.column_names('my_table')
    ['1test', 'test column', 'test!with!exclamation!points', 'test']
    >>> db.column_names('my_table', do_not_decode=True)
    ['__b32__GF2GK43U',
     '__b32__ORSXG5BAMNXWY5LNNY',
     '__b32__ORSXG5BBO5UXI2BBMV4GG3DBNVQXI2LPNYQXA33JNZ2HG',
     'test']

Any column access method will work *ONLY* with the original names, so you do not need to worry about the encoded names.

.. code-block:: python

    >>> db['my_table']['1test'].values
    [1, 2, 3]
    >>> db.get_column('my_table', 'test column').values
    [1, 2, 3]
    >>> db.select('my_table', columns=['test!with!exclamation!points'])
    [(1,), (2,), (3,)]

Where statements also use original column names, and not the encoded ones.

.. code-block:: python

    >>> from dbastable import Where
    >>> db.select('my_table', where={'test!with!exclamation!points': 1})
    [(1, 1, 1)]
    >>> db.select('my_table', where=Where('1test', '>', 1))
    [(2, 2, 2), (3, 3, 3)]
