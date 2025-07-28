from __future__ import annotations

import typing

import absorb


def parse_chunk_path(
    path: str,
    filename_template: str,
    *,
    chunk_size: absorb.ChunkSize | None,
) -> dict[str, typing.Any]:
    import os

    keys = os.path.splitext(filename_template)[0].split('__')
    values = os.path.splitext(os.path.basename(path))[0].split('__')
    items = {k[1:-1]: v for k, v in zip(keys, values)}
    if chunk_size is not None and 'chunk' in items:
        items['chunk'] = parse_chunk(items['chunk'], chunk_size)
    return items


def parse_chunk(as_str: str, chunk_size: absorb.ChunkSize | None) -> typing.Any:
    import datetime

    if chunk_size == 'hour':
        return datetime.datetime.strptime(as_str, '%Y-%m-%d--%H-%M-%S')
    elif chunk_size == 'day':
        return datetime.datetime.strptime(as_str, '%Y-%m-%d')
    elif chunk_size == 'week':
        return datetime.datetime.strptime(as_str, '%Y-%m-%d')
    elif chunk_size == 'month':
        return datetime.datetime.strptime(as_str, '%Y-%m')
    elif chunk_size == 'quarter':
        year = int(as_str[:4])
        month = int(as_str[as_str.index('Q') + 1 :])
        return datetime.datetime(year, month, 1)
    elif chunk_size == 'year':
        return datetime.datetime.strptime(as_str, '%Y')
    else:
        raise NotImplementedError()
