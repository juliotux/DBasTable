

def _sanitize_colnames(data):
    """Sanitize the colnames to avoid invalid characteres like '-'."""
    def _sanitize(key):
        if len([ch for ch in key if not ch.isalnum() and ch != '_']) != 0:
            raise ValueError(f'Invalid column name: {key}.')
        return key.lower()

    if isinstance(data, dict):
        d = data
        colnames = _sanitize_colnames(list(data.keys()))
        return dict(zip(colnames, d.values()))
    if isinstance(data, str):
        return _sanitize(data)
    if not isinstance(data, (list, tuple, np.ndarray)):
        raise TypeError(f'{type(data)} is not supported.')

    return [_sanitize(i) for i in data]


def _sanitize_value(data):
    """Sanitize the value to avoid sql errors."""
    if data is None or isinstance(data, bytes):
        return data
    if isinstance(data, (str, np.str_)):
        return f"{data}"
    if np.isscalar(data) and np.isreal(data):
        if isinstance(data, (int, np.integer)):
            return int(data)
        elif isinstance(data, (float, np.floating)):
            return float(data)
    if isinstance(data, (bool, np.bool_)):
        return bool(data)
    raise TypeError(f'{type(data)} is not supported.')


def _fix_row_index(row, length):
    """Fix the row number to be a valid index."""
    if row < 0:
        row += length
    if row >= length or row < 0:
        raise IndexError('Row index out of range.')
    return row


def _dict2row(cols, **row):
    values = [None]*len(cols)
    for i, c in enumerate(cols):
        if c in row.keys():
            values[i] = row[c]
        else:
            values[i] = None
    return values


def _parse_where(where):
    args = None
    if where is None:
        _where = None
    elif isinstance(where, dict):
        where = _sanitize_colnames(where)
        for i, (k, v) in enumerate(where.items()):
            v = _sanitize_value(v)
            if i == 0:
                _where = f"{k}=?"
                args = [v]
            else:
                _where += f" AND {k}=?"
                args.append(v)
    elif isinstance(where, str):
        _where = where
    elif isinstance(where, (list, tuple)):
        for w in where:
            if not isinstance(w, str):
                raise TypeError('if where is a list, it must be a list '
                                f'of strings. Not {type(w)}.')
        _where = ' AND '.join(where)
    else:
        raise TypeError('where must be a string, list of strings or'
                        ' dict.')
    return _where, args
