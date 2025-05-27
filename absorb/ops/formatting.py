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


def format_chunk(
    data_range: typing.Any, chunk_format: absorb.ChunkFormat
) -> str:
    if chunk_format == 'hour':
        return data_range.strftime('%Y-%m-%d--%H-%M-%S')  # type: ignore
    elif chunk_format == 'day':
        return data_range.strftime('%Y-%m-%d')  # type: ignore
    elif chunk_format == 'week':
        return data_range.strftime('%Y-%m-%d')  # type: ignore
    elif chunk_format == 'month':
        return data_range.strftime('%Y-%m')  # type: ignore
    elif chunk_format == 'quarter':
        if data_range.month == 1 and data_range.day == 1:
            quarter = 1
        elif data_range.month == 4 and data_range.day == 1:
            quarter = 2
        elif data_range.month == 7 and data_range.day == 1:
            quarter = 4
        elif data_range.month == 10 and data_range.day == 1:
            quarter = 4
        else:
            raise Exception('invalid quarter timestamp')
        return data_range.strftime('%Y-Q') + str(quarter)  # type: ignore
    elif chunk_format == 'year':
        return data_range.strftime('%Y')  # type: ignore
    elif chunk_format == 'timestamp':
        return data_range.strftime('%Y-%m-%d--%H-%M-%S')  # type: ignore
    elif chunk_format == 'timestamp_range':
        import datetime

        t_start: datetime.datetime
        t_end: datetime.datetime
        t_start, t_end = data_range
        return (
            t_start.strftime('%Y-%m-%d--%H-%M-%S')
            + '_to_'
            + t_end.strftime('%Y-%m-%d--%H-%M-%S')
        )
    elif chunk_format == 'number':
        width = 10
        template = '%0' + str(width) + 'd'
        return template % data_range  # type: ignore
    elif chunk_format == 'number_range':
        width = 10
        template = '%0' + str(width) + 'd'
        start, end = data_range
        return (template % start) + '_to_' + (template % end)  # type: ignore
    elif chunk_format == 'name':
        return data_range  # type: ignore
    elif chunk_format == 'name_list':
        return '_'.join(data_range)
    elif chunk_format is None:
        return str(data_range)
    elif chunk_format == 'all':
        return 'all'
    else:
        raise Exception('invalid chunk range format: ' + str(chunk_format))
