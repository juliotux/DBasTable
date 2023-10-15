DBasTable
---------

Handle SQLite database tables just as if they were `Numpy structured arrays <https://numpy.org/doc/stable/user/basics.rec.html>`_, `~astropy.table.Table` or `~pandas.DataFrame`.

.. code-block:: python

    >>> from dbastable import SQLDatabase
    >>> db = SQLDatabase('test.db', autocommit=True)
    >>> db.add_table('table1')
    >>> db['table1']['col1'] = [1, 2, 3, 4]
    >>> db['table1']['col2'] = ['a', 'b', 'c', 'd']
    >>> print(db['table1'][2].values)
    (3, 'c')


Installation
------------

The easiest way to install dbastable is via `pip <https://pip.pypa.io/en/stable/>`_::

    pip install dbastable

Alternatively, you can clone the repository and install it manually::

    git clone
    cd dbastable
    pip install -U .

or

    pip install -U git+https://github.com/juliotux/dbastable


Documentation
-------------

The documentation is available at https://dbastable.readthedocs.io/en/latest/


License
-------

`dbastable` is licensed under the terms of the `MIT license <https://opensource.org/license/mit/>`_. See the file "LICENSE" for information on the history of this software, terms & conditions for usage, and a DISCLAIMER OF ALL WARRANTIES.

API Reference
-------------

.. toctree::
   :maxdepth: 1

   api
