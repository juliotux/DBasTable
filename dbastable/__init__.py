# Licensed under a 3-clause BSD style license - see LICENSE.rst
"""Manage SQL databases in a simplier way."""

try:
    from .version import version as __version__
except ImportError:
    __version__ = ''

from ._sqldb import SQLDatabase, SQLTable, SQLRow, SQLColumn
from . import where
from .tests import run_tests

__all__ = ['SQLDatabase', 'SQLTable', 'SQLRow', 'SQLColumn',
           'where', 'run_tests', '__version__']
