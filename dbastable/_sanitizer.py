import numpy as np
import base64

from ._def import _B32_COL_PREFIX, _ID_KEY


def _colname_to_b32_decode(string):
    """Fix the encoded colname string to b32 proper decoding."""
    # ensure remove the prefix
    string = string.lstrip(_B32_COL_PREFIX)
    # base32 need 8 chars padding
    remainder = len(string) % 8
    string += '='*(8-remainder)
    return string


class _SanitizerMixin:
    """Mixin class to add functions related to SQL sanitization."""

    def _encode_b32(self, key):
        """Encode a key in base32."""
        key = key.casefold()
        # encode to base32 and remove the padding
        b = base64.b32encode(key.encode('utf-8')).decode('utf-8')
        b = b.strip('=')
        # add the prefix to recognize it later
        return f'{_B32_COL_PREFIX}{b}'

    def _decode_b32(self, key):
        """Decode a key from base32."""
        b = _colname_to_b32_decode(key)
        return base64.b32decode(b).decode('utf-8')

    def _sanitize_key(self, key):
        """Sanitize a single key to avoid sql errors."""
        # TODO: check for protected names
        if key.startswith(_B32_COL_PREFIX) or key == _ID_KEY:
            raise ValueError(f'{key} uses a protected name.')
        if len([ch for ch in key if not ch.isalnum() and ch != '_']) != 0:
            # if a name is invalid, encode it in base32 and add a prefix
            # if it is allowed
            if self._allow_b32_colnames:
                return self._encode_b32(key)
            raise ValueError(f'Invalid column name: {key}.')
        return key.lower()

    def _sanitize_colnames(self, data):
        """Sanitize the colnames to avoid invalid characteres like '-'."""

        # for dictionaries, we sanitize the names and keep the values here
        if isinstance(data, dict):
            d = data
            colnames = self._sanitize_colnames(list(data.keys()))
            return dict(zip(colnames, d.values()))

        # single string sanitization must not be returned as a list
        if isinstance(data, str):
            return self._sanitize_key(data)

        # for lists, tuples and numpy arrays, we sanitize each element
        if isinstance(data, (list, tuple, np.ndarray)):
            return [self._sanitize_key(i) for i in data]

        raise TypeError(f'{type(data)} is not supported.')

    @staticmethod
    def _sanitize_value(data):
        """Sanitize the value to avoid sql errors."""
        # Bytes is allowed without changes
        if data is None or isinstance(data, bytes):
            return data

        # For strings, ensure it is a string in the proper format
        if isinstance(data, (str, np.str_)):
            return f"{data}"

        # For numbers, ensure it is a number in the proper format
        if np.isscalar(data) and np.isreal(data):
            if isinstance(data, (int, np.integer)):
                return int(data)
            elif isinstance(data, (float, np.floating)):
                return float(data)

        # For booleans, ensure it is a boolean in the proper format
        if isinstance(data, (bool, np.bool_)):
            return bool(data)

        raise TypeError(f'{type(data)} is not supported.')

    def _get_column_name(self, table, column):
        """Get the real column name from the database."""
        if not isinstance(column, str):
            raise TypeError('column must be a string.')

        # casefold to make it case insensitive
        column = column.casefold()
        if column.casefold() == _ID_KEY:
            return column.casefold()  # id key is already sanitized

        # get the sanitized column name
        col = self._sanitize_colnames(column)
        # if the column is not in the database, raise an error
        if column not in self.column_names(table):
            raise ValueError(f'Column {column} not found in the database.')

        return str(col)
