from __future__ import annotations

import typing

import absorb


bullet_styles = {
    'key_style': 'white bold',
    'bullet_style': 'green',
    'colon_style': 'green',
}


def print_bullet(
    key: str | None,
    value: str | None,
    symbol_color: str | None = None,
    **kwargs: typing.Any,
) -> None:
    import toolstr

    if symbol_color is not None:
        styles = bullet_styles.copy()
        styles['bullet_style'] = symbol_color
        styles['colon_style'] = symbol_color
    else:
        styles = bullet_styles

    toolstr.print_bullet(key=key, value=value, **kwargs, **styles)


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


def format_bytes(bytes_size: int | float) -> str:
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_size < 1024.0:
            return f'{bytes_size:.2f} {unit}'
        bytes_size /= 1024.0
    return f'{bytes_size:.2f} PB'


def preview(
    dataset: absorb.TableReference, offset: int | None, n_rows: int | None
) -> None:
    import polars as pl
    import toolstr

    if offset is None:
        offset = 0
    if n_rows is None:
        n_rows = 10

    pl.Config.set_tbl_hide_dataframe_shape(True)
    pl.Config.set_tbl_rows(n_rows)

    # load dataset preview
    df = absorb.query(dataset).slice(offset).head(n_rows + 1)

    # print number of rows in preview
    dataset = absorb.Table.instantiate(dataset)
    toolstr.print_text_box(dataset.full_name(), style='bold')

    if len(df) > n_rows:
        if offset > 0:
            print(n_rows, 'rows starting from offset', offset)
        else:
            print('first', n_rows, 'rows:')

    # print dataset preview
    print(df.head(n_rows))

    # print total number of rows
    dataset_n_rows = absorb.ops.scan(dataset).select(pl.len()).collect().item()
    print(dataset_n_rows, 'rows,', len(df.columns), 'columns')


def print_table_info(
    table: absorb.Table, verbose: bool
) -> dict[str, typing.Any]:
    import os
    import toolstr

    schema = table.get_schema()

    toolstr.print_text_box(
        'Table = ' + table.name(),
        style='green',
        text_style='bold white',
    )

    for attr in [
        'description',
        'url',
        'source',
        'write_range',
    ]:
        if hasattr(table, attr):
            value = getattr(table, attr)
        else:
            value = None
        absorb.ops.print_bullet(key=attr, value=value)
    absorb.ops.print_bullet(key='chunk_size', value=str(table.get_chunk_size()))

    # parameters
    print()
    toolstr.print('[green bold]parameters[/green bold]')
    if table.parameters is None or len(table.parameter_types) == 0:
        print('- [none]')
    else:
        for key, value in table.parameter_types.items():
            if key in table.default_parameters:
                default = (
                    ' \\[default = ' + str(table.default_parameters[key]) + ']'
                )
            else:
                default = ''
            absorb.ops.print_bullet(key=key, value=str(value) + default)

    # schema
    print()
    toolstr.print('[green bold]schema[/green bold]')
    for key, value in schema.items():
        absorb.ops.print_bullet(key=key, value=str(value))

    # collection status
    print()
    toolstr.print('[green bold]status[/green bold]')
    if not table.is_collected():
        print('- [not collected]')
    else:
        # print available range
        if verbose:
            available_range = table.get_available_range()
            if available_range is not None:
                formatted_available_range = absorb.ops.format_coverage(
                    available_range, table.get_chunk_size()
                )
            else:
                formatted_available_range = 'not available'
            absorb.ops.print_bullet(
                key='available range',
                value=formatted_available_range,
            )

        # print collected range
        collected_range = table.get_collected_range()
        absorb.ops.print_bullet(
            key='collected range',
            value=absorb.ops.format_coverage(
                collected_range, table.get_chunk_size()
            ),
        )

        # print path and collected size
        path = table.get_table_dir()
        bytes_str = absorb.ops.format_bytes(absorb.ops.get_dir_size(path))
        absorb.ops.print_bullet(key='path', value=path)
        absorb.ops.print_bullet(key='size', value=bytes_str)

    return {}
