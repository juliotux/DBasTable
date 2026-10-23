# Licensed under a 3-clause BSD style license - see LICENSE.rst
"""Manage SQL databases in a simplier way."""

try:
    from .version import version as __version__
except ImportError:
    __version__ = ''

from ._def import _ID_KEY
from ._sqldb import SQLColumn, SQLDatabase, SQLRow, SQLTable
from .where import Where

__all__ = [
    '_ID_KEY',
    'SQLColumn',
    'SQLDatabase',
    'SQLRow',
    'SQLTable',
    'Where',
    '__version__',
]
