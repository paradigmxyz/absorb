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
    coverage: absorb.Coverage | None, index_type: absorb.IndexType | None
) -> str:
    if coverage is None:
        return 'None'
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


def format_chunk(
    chunk: absorb.Chunk, index_type: absorb.IndexType | None
) -> str:
    if chunk is None:
        return '-'
    if index_type is None:
        return '-'

    if index_type == 'hour':
        return chunk.strftime('%Y-%m-%d--%H-%M-%S')  # type: ignore
    elif index_type == 'day':
        return chunk.strftime('%Y-%m-%d')  # type: ignore
    elif index_type == 'week':
        return chunk.strftime('%Y-%m-%d')  # type: ignore
    elif index_type == 'month':
        return chunk.strftime('%Y-%m')  # type: ignore
    elif index_type == 'quarter':
        if chunk.month == 1 and chunk.day == 1:  # type: ignore
            quarter = 1
        elif chunk.month == 4 and chunk.day == 1:  # type: ignore
            quarter = 2
        elif chunk.month == 7 and chunk.day == 1:  # type: ignore
            quarter = 4
        elif chunk.month == 10 and chunk.day == 1:  # type: ignore
            quarter = 4
        else:
            raise Exception('invalid quarter timestamp')
        return chunk.strftime('%Y-Q') + str(quarter)  # type: ignore
    elif index_type == 'year':
        return chunk.strftime('%Y')  # type: ignore
    elif index_type == 'timestamp':
        return chunk.strftime('%Y-%m-%d--%H-%M-%S')  # type: ignore
    elif index_type == 'timestamp_range':
        import datetime

        t_start: datetime.datetime
        t_end: datetime.datetime
        t_start, t_end = chunk  # type: ignore
        return (
            t_start.strftime('%Y-%m-%d--%H-%M-%S')
            + '_to_'
            + t_end.strftime('%Y-%m-%d--%H-%M-%S')
        )
    elif index_type == 'number':
        width = 10
        template = '%0' + str(width) + 'd'
        return template % chunk
    elif index_type == 'number_range':
        width = 10
        template = '%0' + str(width) + 'd'
        start, end = chunk  # type: ignore
        return (template % start) + '_to_' + (template % end)
    elif index_type == 'id':
        return chunk  # type: ignore
    elif index_type == 'id_list':
        return '_'.join(chunk)  # type: ignore
    elif index_type is None:
        return str(chunk)
    else:
        raise Exception('invalid chunk range format: ' + str(index_type))
