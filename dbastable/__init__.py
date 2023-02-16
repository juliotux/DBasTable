# Licensed under a 3-clause BSD style license - see LICENSE.rst
"""Manage SQL databases in a simplier way."""

import sqlite3 as sql
import numpy as np
from astropy.table import Table


__all__ = ['SQLDatabase', 'SQLTable', 'SQLRow', 'SQLColumn', 'SQLColumnMap']


_ID_KEY = '__id__'
