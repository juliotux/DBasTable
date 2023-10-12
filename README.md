# DBasTable

[![Unit Tests](https://github.com/juliotux/DBasTable/actions/workflows/unittests.yml/badge.svg)](https://github.com/juliotux/DBasTable/actions/workflows/unittests.yml)

A simplier way to access SQLite tables, just like Numpy structured arrarys or Pandas dataframes.

```
>>> from dbastable import SQLDatabase
>>> db = SQLDatabase('test.db', autocommit=True)
>>> db.add_table('table1')
>>> db['table1']['col1'] = [1, 2, 3, 4]
>>> db['table1']['col2'] = ['a', 'b', 'c', 'd']
>>> print(db['table1'][2].values)
(3, 'c')
```

Its design is based mainly in the way you interact with [Numpy structured arrays](https://numpy.org/doc/stable/user/basics.rec.html), [Pandas dataframes](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html) and [Astropy Tables](https://docs.astropy.org/en/stable/table/index.html), while keep the possibility of non-volatile and in-disk data storage.

## What is it for?

The design of this package was made for *very simple interactions* with Python's sqlite implementation, so you can interact with sqlite databases without know SQL commands. So, if you need to work with simple tables, composed by columns of data in standard python formats and don't want to perform SQL queries manually, may be this package is for you.

## What is it *NOT* for?

As menioned above, we intended to perform only simple operations with this package. So, *we intentionally limited the functionality*. Do not expect perform complex queries here. This package is for simplicity. There are several alternatives that are more feature-complete.

## Do not use in large production

I'm not a SQL master, nor a digital security guru. I'm an astrophysicist that do some python. So, if you want to use it, use with care.

# Install and Documentation
