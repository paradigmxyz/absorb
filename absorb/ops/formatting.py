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


def format_coverage(
    coverage: absorb.Coverage, index_type: absorb.IndexType
) -> str:
    if isinstance(coverage, tuple):
        start, end = coverage
        return (
            format_chunk(start, index_type)
            + '_to_'
            + format_chunk(end, index_type)
        )
    elif isinstance(coverage, list):
        start = min(coverage)
        end = max(coverage)
        return (
            format_chunk(start, index_type)
            + '_to_'
            + format_chunk(end, index_type)
        )
    elif isinstance(coverage, dict):
        raise NotImplementedError()
    else:
        raise Exception()


def format_chunk(data_range: typing.Any, index_type: absorb.IndexType) -> str:
    if index_type == 'hour':
        return data_range.strftime('%Y-%m-%d--%H-%M-%S')  # type: ignore
    elif index_type == 'day':
        return data_range.strftime('%Y-%m-%d')  # type: ignore
    elif index_type == 'week':
        return data_range.strftime('%Y-%m-%d')  # type: ignore
    elif index_type == 'month':
        return data_range.strftime('%Y-%m')  # type: ignore
    elif index_type == 'quarter':
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
    elif index_type == 'year':
        return data_range.strftime('%Y')  # type: ignore
    elif index_type == 'timestamp':
        return data_range.strftime('%Y-%m-%d--%H-%M-%S')  # type: ignore
    elif index_type == 'timestamp_range':
        import datetime

        t_start: datetime.datetime
        t_end: datetime.datetime
        t_start, t_end = data_range
        return (
            t_start.strftime('%Y-%m-%d--%H-%M-%S')
            + '_to_'
            + t_end.strftime('%Y-%m-%d--%H-%M-%S')
        )
    elif index_type == 'number':
        width = 10
        template = '%0' + str(width) + 'd'
        return template % data_range  # type: ignore
    elif index_type == 'number_range':
        width = 10
        template = '%0' + str(width) + 'd'
        start, end = data_range
        return (template % start) + '_to_' + (template % end)  # type: ignore
    elif index_type == 'name':
        return data_range  # type: ignore
    elif index_type == 'name_list':
        return '_'.join(data_range)
    elif index_type is None:
        return str(data_range)
    else:
        raise Exception('invalid chunk range format: ' + str(index_type))
