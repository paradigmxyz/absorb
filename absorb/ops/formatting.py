from __future__ import annotations

import typing

import absorb


bullet_styles = {
    'key_style': 'white bold',
    'bullet_style': 'green',
    'colon_style': 'green',
}


def print_bullet(
    key: str | None, value: str | None, **kwargs: typing.Any
) -> None:
    import toolstr

    toolstr.print_bullet(key=key, value=value, **kwargs, **bullet_styles)


def format_chunk(chunk: typing.Any, chunk_format: absorb.ChunkFormat) -> str:
    import datetime

    if isinstance(chunk, list):
        date_strs = [
            '-' if dt is None else dt.strftime('%Y-%m-%d') for dt in chunk
        ]
        return '\[' + ', '.join(date_strs) + ']'
    elif isinstance(chunk, datetime.datetime):
        return chunk.strftime('%Y-%m-%d')
    else:
        return str(chunk)
