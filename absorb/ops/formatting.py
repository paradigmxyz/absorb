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
    coverage: absorb.Coverage | None, chunk_size: absorb.ChunkSize | None
) -> str:
    if coverage is None:
        return 'None'
    if isinstance(coverage, tuple):
        start, end = coverage
        return (
            format_chunk(start, chunk_size)
            + '_to_'
            + format_chunk(end, chunk_size)
        )
    elif isinstance(coverage, list):
        start = min(coverage)[0]
        end = max(coverage)[0]
        return (
            format_chunk(start, chunk_size)
            + '_to_'
            + format_chunk(end, chunk_size)
        )
    elif isinstance(coverage, dict):
        raise NotImplementedError()
    else:
        raise Exception()


def format_chunk(
    chunk: absorb.Chunk, chunk_size: absorb.ChunkSize | None
) -> str:
    if chunk is None:
        return '-'
    if chunk_size is None:
        return str(chunk)

    if chunk_size == 'hour':
        return chunk.strftime('%Y-%m-%d--%H-%M-%S')  # type: ignore
    elif chunk_size == 'day':
        return chunk.strftime('%Y-%m-%d')  # type: ignore
    elif chunk_size == 'week':
        return chunk.strftime('%Y-%m-%d')  # type: ignore
    elif chunk_size == 'month':
        return chunk.strftime('%Y-%m')  # type: ignore
    elif chunk_size == 'quarter':
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
    elif chunk_size == 'year':
        return chunk.strftime('%Y')  # type: ignore
    elif isinstance(chunk_size, int):
        width = 10
        template = '%0' + str(width) + 'd'
        return template % chunk
    elif isinstance(chunk_size, dict):
        raise NotImplementedError('chunk_size as dict not implemented')
    else:
        raise Exception('invalid chunk_size format: ' + str(type(chunk_size)))
