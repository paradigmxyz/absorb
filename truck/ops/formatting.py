from __future__ import annotations

import typing


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


def format_range(data_range: typing.Any) -> str:
    import datetime

    if isinstance(data_range, list):
        date_strs = [
            '-' if dt is None else dt.strftime('%Y-%m-%d') for dt in data_range
        ]
        return '\[' + ', '.join(date_strs) + ']'
    elif isinstance(data_range, datetime.datetime):
        return data_range.strftime('%Y-%m-%d')
    else:
        return str(data_range)
