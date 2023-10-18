Where Statements and Queries
============================

Any method that receives a ``where`` argument, like `SQLDatabase.select` or `SQLDatabase.count` will work in the same way. To support a wide range of queries, the ``where`` argument can be a dictionary or a `~dbastable.Where` object.

The dictionary method is the simplest, and is the one used in the examples above. The dictionary keys are the column names, and the values are the values to match. Using dictionary the equality is always assumed.

.. code-block:: python

    >>> # let's create a db to play with
    >>> from dbastable import SQLDatabase
    >>> db = SQLDatabase()
    >>> db.add_table('table1', columns=['id', 'name', 'value'])
    >>> db.add_rows('table1', [[1, 'foo', 10],
    ...                        [2, 'bar', 20],
    ...                        [3, 'baz', 15],
    ...                        [4, 'qux', 20],
    ...                        [5, 'tux', 10]])
    >>> db.select('table1', columns='name', where={'value': 20})
    [('bar',), ('qux',)]

Multiple statements are supported. They will be combined using the ``AND`` operator. ``OR`` is not supported.

.. code-block:: python

    >>> db.select('table1', columns='name', where={'value': 20, 'id': 4})
    [('qux',)]

The `~dbastable.Where` object allows for more complex queries. It supports the following operators:

* ``=``: equality
* ``!=``: inequality
* ``<``: less than
* ``<=``: less than or equal to
* ``>``: greater than
* ``>=``: greater than or equal to
* ``LIKE``: SQL ``LIKE`` operator
* ``IN``: SQL ``IN`` operator
* ``NOT IN``: SQL ``NOT IN`` operator
* ``IS``: SQL ``IS`` operator
* ``IS NOT``: SQL ``IS NOT`` operator
* ``BETWEEN``: SQL ``BETWEEN`` operator
* ``NOT BETWEEN``: SQL ``NOT BETWEEN`` operator

This `~dbastable.Where` object can be used directly as the ``where`` argument, as value in `dict`. Also, if a list is passed to the ``where`` argument, it must be a list of `~dbastable.Where` object that will be combined with ``AND``.

.. code-block:: python

    >>> from dbastable import Where
    >>> db.select('table1', columns='name',
    ...           where={'value': Where('value', '>', 15)})
    [('bar',), ('qux',)]
    >>> db.select('table1', columns='name',
    ...           where=[Where('value', 'IN', [10, 15]),
    ...                  Where('id', '>', 3)])
    [('tux',)]
    >>> db.select('table1', where=Where('value', 'BETWEEN', [15, 25]))
    [(2, 'bar', 20), (3, 'baz', 15), (4, 'qux', 20)]
