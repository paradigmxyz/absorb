from __future__ import annotations

import typing

import absorb


_cache = {'root_dir_warning_shown': False}


def get_absorb_root(*, warn: bool = False) -> str:
    import os

    path = os.environ.get('TRUCK_ROOT')
    if path is None or path == '':
        if warn and not _cache['root_dir_warning_shown']:
            import rich

            rich.print(
                '[#777777]using default value for TRUCK_ROOT: ~/absorb[/#777777]\n'
            )
            _cache['root_dir_warning_shown'] = True
        path = '~/absorb'
    path = os.path.expanduser(path)
    return path


def get_config_path(*, warn: bool = False) -> str:
    import os

    return os.path.join(
        absorb.ops.get_absorb_root(warn=warn), 'absorb_config.json'
    )


def get_source_dir(source: str, *, warn: bool = False) -> str:
    import os

    return os.path.join(get_absorb_root(warn=warn), 'datasets', source)


def get_table_dir(
    dataset: absorb.TrackedTable | str | None = None,
    *,
    source: str | None = None,
    table: str | None = None,
    warn: bool = False,
) -> str:
    import os

    if isinstance(dataset, str):
        source, table = dataset.split('.')
    elif isinstance(dataset, dict):
        source = dataset['source_name']
        table = dataset['table_name']
    elif source is not None and table is not None:
        pass
    else:
        raise Exception('invalid format')

    source_dir = get_source_dir(source, warn=warn)
    return os.path.join(source_dir, 'tables', table)


def get_table_metadata_path(
    dataset: absorb.TrackedTable | str, *, warn: bool = False
) -> str:
    import os

    return os.path.join(
        absorb.ops.get_table_dir(dataset, warn=warn), 'table_metadata.json'
    )


def get_table_filepath(
    data_range: typing.Any,
    chunk_format: absorb.ChunkFormat,
    filename_template: str,
    table: str,
    *,
    source: str | None,
    parameters: dict[str, typing.Any],
    glob: bool = False,
    warn: bool = True,
) -> str:
    import os

    dir_path = get_table_dir(source=source, table=table, warn=warn)
    filename = get_table_filename(
        data_range=data_range,
        chunk_format=chunk_format,
        filename_template=filename_template,
        table=table,
        source=source,
        parameters=parameters,
        glob=glob,
    )
    return os.path.join(dir_path, filename)


def get_table_filename(
    data_range: typing.Any,
    chunk_format: absorb.ChunkFormat,
    filename_template: str,
    table: str,
    *,
    source: str | None,
    parameters: dict[str, typing.Any],
    glob: bool = False,
) -> str:
    format_params = parameters.copy()
    if source is not None:
        format_params['source'] = source
    format_params['table'] = table
    if '{data_range}' in filename_template:
        if glob:
            format_params['data_range'] = '*'
        else:
            format_params['data_range'] = _format_data_range(
                data_range, chunk_format
            )
    return filename_template.format(**format_params)


def get_table_filepaths(
    data_ranges: typing.Any,
    chunk_format: absorb.ChunkFormat,
    filename_template: str,
    table: str,
    *,
    source: str | None,
    parameters: dict[str, typing.Any],
    warn: bool = True,
) -> list[str]:
    import os

    dir_path = get_table_dir(source=source, table=table, warn=warn)
    paths = []
    for data_range in data_ranges:
        filename = get_table_filename(
            data_range=data_range,
            chunk_format=chunk_format,
            filename_template=filename_template,
            table=table,
            source=source,
            parameters=parameters,
        )
        path = os.path.join(dir_path, filename)
        paths.append(path)
    return paths


def _format_data_range(
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
        return (
            _format_value(data_range[0], 'date')
            + '_to_'
            + _format_value(data_range[1], 'date')
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
    elif chunk_format == 'name_range':
        start, end = data_range
        return start + '_to_' + end  # type: ignore
    elif chunk_format == 'name_list':
        return '_'.join(data_range)
    elif chunk_format is None:
        return str(data_range)
    else:
        raise Exception('invalid chunk range format: ' + str(chunk_format))


def _format_value(
    value: typing.Any, format: str | None = None, width: int = 10
) -> str:
    if format is None:
        import datetime

        if isinstance(value, datetime.datetime):
            format = 'date'
        elif isinstance(value, int):
            format = 'int'
        else:
            raise Exception('invalid format')

    if format == 'date':
        return value.strftime('%Y-%m-%d')  # type: ignore
    elif format == 'int':
        return ('%0' + str(width) + 'd') % value  # type: ignore
    else:
        raise Exception('invalid format')


def parse_file_path(
    path: str,
    filename_template: str,
    *,
    chunk_format: absorb.ChunkFormat | None = None,
) -> dict[str, typing.Any]:
    import os

    keys = os.path.splitext(filename_template)[0].split('__')
    values = os.path.splitext(os.path.basename(path))[0].split('__')
    items = {k[1:-1]: v for k, v in zip(keys, values)}
    if chunk_format is not None and 'data_range' in items:
        items['data_range'] = parse_data_range(
            items['data_range'], chunk_format
        )
    return items


def parse_data_range(
    as_str: str, chunk_format: absorb.ChunkFormat
) -> typing.Any:
    if chunk_format == 'timestamp':
        import datetime

        return datetime.datetime.strptime(as_str, '%Y-%m-%d')
    else:
        raise NotImplementedError()
