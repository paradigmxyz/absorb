from __future__ import annotations

import typing

import truck


_cache = {'root_dir_warning_shown': False}


def get_truck_root(*, warn: bool = False) -> str:
    import os

    path = os.environ.get('TRUCK_ROOT')
    if path is None or path == '':
        if warn and not _cache['root_dir_warning_shown']:
            import rich

            rich.print(
                '[#777777]using default value for TRUCK_ROOT: ~/truck[/#777777]\n'
            )
            _cache['root_dir_warning_shown'] = True
        path = '~/truck'
    path = os.path.expanduser(path)
    return path


def get_config_path(*, warn: bool = False) -> str:
    import os

    return os.path.join(truck.get_truck_root(warn=warn), 'truck_config.json')


def get_source_dir(source: str, *, warn: bool = False) -> str:
    import os

    return os.path.join(get_truck_root(warn=warn), 'datasets', source)


def get_table_dir(
    dataset: truck.TrackedTable | str | None = None,
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
    dataset: truck.TrackedTable | str, *, warn: bool = False
) -> str:
    import os

    return os.path.join(
        truck.get_table_dir(dataset, warn=warn), 'table_metadata.json'
    )


def get_table_filepath(
    data_range: typing.Any,
    range_format: truck.RangeFormat,
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
        range_format=range_format,
        filename_template=filename_template,
        table=table,
        source=source,
        parameters=parameters,
        glob=glob,
    )
    return os.path.join(dir_path, filename)


def get_table_filename(
    data_range: typing.Any,
    range_format: truck.RangeFormat,
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
                data_range, range_format
            )
    return filename_template.format(**format_params)


def get_table_filepaths(
    data_ranges: typing.Any,
    range_format: truck.RangeFormat,
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
            range_format=range_format,
            filename_template=filename_template,
            table=table,
            source=source,
            parameters=parameters,
        )
        path = os.path.join(dir_path, filename)
        paths.append(path)
    return paths


def _format_data_range(
    data_range: typing.Any, range_format: truck.RangeFormat
) -> str:
    if range_format == 'date':
        return data_range.strftime('%Y-%m-%d')  # type: ignore
    elif range_format == 'date_range':
        return (
            _format_value(data_range[0], 'date')
            + '_to_'
            + _format_value(data_range[1], 'date')
        )
    elif range_format == 'named_range':
        if len(data_range) == 2:
            values = list(data_range.values())
            values = sorted(values)
            values_str = [_format_value(value) for value in values]
            return values_str[0] + '_to_' + values_str[1]
        else:
            raise NotImplementedError('range with >2 keys')
    elif range_format == 'block_range':
        return (
            _format_value(data_range[0], 'int')
            + '_to_'
            + _format_value(data_range[1], 'int')
        )
    elif range_format == 'id_range':
        return (
            _format_value(data_range[0]) + '_to_' + _format_value(data_range[1])
        )
    elif range_format is None:
        return str(data_range)
    else:
        raise Exception('invalid range_format: ' + str(range_format))


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
    range_format: truck.RangeFormat | None = None,
) -> dict[str, typing.Any]:
    import os

    keys = os.path.splitext(filename_template)[0].split('__')
    values = os.path.splitext(os.path.basename(path))[0].split('__')
    items = {k[1:-1]: v for k, v in zip(keys, values)}
    if range_format is not None and 'data_range' in items:
        items['data_range'] = parse_data_range(
            items['data_range'], range_format
        )
    return items


def parse_data_range(
    as_str: str, range_format: truck.RangeFormat
) -> typing.Any:
    raise NotImplementedError()
