from __future__ import annotations

import typing

import absorb


_cache = {'root_dir_warning_shown': False}


def get_absorb_root(*, warn: bool = False) -> str:
    import os

    path = os.environ.get('ABSORB_ROOT')
    if path is None or path == '':
        if warn and not _cache['root_dir_warning_shown']:
            import rich

            rich.print(
                '[#777777]using default value for ABSORB_ROOT: ~/absorb\n(set a value for the ABSORB_ROOT env var to remove this message)[/#777777]'
            )
            _cache['root_dir_warning_shown'] = True
        path = '~/absorb'
    path = os.path.expanduser(path)
    return path


def set_absorb_root(path: str) -> None:
    import os

    os.environ['ABSORB_ROOT'] = path
    _cache['root_dir_warning_shown'] = False


def get_config_path(*, warn: bool = False) -> str:
    import os

    return os.path.join(
        absorb.ops.get_absorb_root(warn=warn), 'absorb_config.json'
    )


def get_datasets_dir(*, warn: bool = False) -> str:
    import os

    return os.path.join(get_absorb_root(warn=warn), 'datasets')


def get_source_tables_dir(source: str, *, warn: bool = False) -> str:
    import os

    return os.path.join(get_datasets_dir(warn=warn), source, 'tables')


def get_source_dir(source: str, *, warn: bool = False) -> str:
    import os

    return os.path.join(get_datasets_dir(warn=warn), source)


def get_table_dir(
    table: str | absorb.TableDict | absorb.Table,
    *,
    source: str | None = None,
    warn: bool = False,
) -> str:
    import os

    if isinstance(table, str):
        if '.' in table:
            source, table = table.split('.')
        else:
            if source is None:
                raise Exception('source must be provided if table is a string')
    elif isinstance(table, dict):
        source = table['source_name']
        table = table['table_name']
    elif isinstance(table, absorb.Table):
        source = table.source
        table = table.name()
    else:
        raise Exception('invalid format')

    source_dir = get_source_dir(source, warn=warn)
    return os.path.join(source_dir, 'tables', table)


def get_table_metadata_path(
    table: str | absorb.TableDict | absorb.Table,
    *,
    source: str | None = None,
    warn: bool = False,
) -> str:
    import os

    table_dir = absorb.ops.get_table_dir(table, source=source, warn=warn)
    return os.path.join(table_dir, 'table_metadata.json')


def get_table_filepath(
    chunk: absorb.Chunk,
    index_type: absorb.IndexType | None,
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
        chunk=chunk,
        index_type=index_type,
        filename_template=filename_template,
        table=table,
        source=source,
        parameters=parameters,
        glob=glob,
    )
    return os.path.join(dir_path, filename)


def get_table_filename(
    chunk: absorb.Chunk,
    index_type: absorb.IndexType | None,
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
    if '{chunk}' in filename_template:
        if glob:
            format_params['chunk'] = '*'
        else:
            if index_type is None:
                raise Exception(
                    'index_type must be provided if {chunk} is in filename_template'
                )
            if isinstance(chunk, str):
                chunk_str = chunk
            else:
                chunk_str = absorb.ops.format_chunk(chunk, index_type)
            format_params['chunk'] = chunk_str
    return filename_template.format(**format_params)


def get_table_filepaths(
    chunks: typing.Any,
    index_type: absorb.IndexType | None,
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
    for chunk in chunks:
        filename = get_table_filename(
            chunk=chunk,
            index_type=index_type,
            filename_template=filename_template,
            table=table,
            source=source,
            parameters=parameters,
        )
        path = os.path.join(dir_path, filename)
        paths.append(path)
    return paths


def parse_file_path(
    path: str,
    filename_template: str,
    *,
    index_type: absorb.IndexType | None = None,
) -> dict[str, typing.Any]:
    import os

    keys = os.path.splitext(filename_template)[0].split('__')
    values = os.path.splitext(os.path.basename(path))[0].split('__')
    items = {k[1:-1]: v for k, v in zip(keys, values)}
    if index_type is not None and 'chunk' in items:
        items['chunk'] = parse_chunk(items['chunk'], index_type)
    return items


def parse_chunk(as_str: str, index_type: absorb.IndexType) -> typing.Any:
    import datetime

    if index_type == 'hour':
        return datetime.datetime.strptime(as_str, '%Y-%m-%d--%H-%M-%S')
    elif index_type == 'day':
        return datetime.datetime.strptime(as_str, '%Y-%m-%d')
    elif index_type == 'week':
        return datetime.datetime.strptime(as_str, '%Y-%m-%d')
    elif index_type == 'month':
        return datetime.datetime.strptime(as_str, '%Y-%m')
    elif index_type == 'quarter':
        year = int(as_str[:4])
        month = int(as_str[as_str.index('Q') + 1 :])
        return datetime.datetime(year, month, 1)
    elif index_type == 'year':
        return datetime.datetime.strptime(as_str, '%Y')
    elif index_type == 'timestamp':
        import datetime

        return datetime.datetime.strptime(as_str, '%Y-%m-%d')
    else:
        raise NotImplementedError()


def get_dir_size(path: str) -> int:
    import platform
    import subprocess

    system = platform.system()

    if system == 'Linux':
        # Linux has -b flag for bytes
        result = subprocess.run(
            ['du', '-sb', path], capture_output=True, text=True, check=True
        )
        return int(result.stdout.strip().split('\t')[0])

    elif system == 'Darwin':  # macOS
        # macOS outputs in 512-byte blocks
        result = subprocess.run(
            ['du', '-s', path], capture_output=True, text=True, check=True
        )
        blocks = int(result.stdout.strip().split('\t')[0])
        return blocks * 512

    else:
        raise NotImplementedError(
            'Unsupported operating system for get_dir_size'
        )


def format_bytes(bytes_size: int | float) -> str:
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_size < 1024.0:
            return f'{bytes_size:.2f} {unit}'
        bytes_size /= 1024.0
    return f'{bytes_size:.2f} PB'


def delete_table_dir(table: absorb.Table, confirm: bool = False) -> None:
    import os
    import shutil

    if not confirm:
        raise absorb.ConfirmError(
            'use confirm=True to delete table and its data files'
        )

    table_dir = table.get_table_dir()
    if os.path.isdir(table_dir):
        shutil.rmtree(table_dir)

    if absorb.ops.get_config()['use_git']:
        absorb.ops.git_remove_and_commit_file(
            absorb.ops.get_table_metadata_path(table),
            repo_root=absorb.ops.get_absorb_root(),
            message='Remove table metadata for ' + table.full_name(),
        )
