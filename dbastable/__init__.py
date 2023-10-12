# Licensed under a 3-clause BSD style license - see LICENSE.rst
"""Manage SQL databases in a simplier way."""

try:
    from .version import version as __version__
except ImportError:
    __version__ = ''

from ._sqldb import SQLDatabase, SQLTable, SQLRow, SQLColumn
from . import where

__all__ = ['SQLDatabase', 'SQLTable', 'SQLRow', 'SQLColumn',
           'where', '__version__']
