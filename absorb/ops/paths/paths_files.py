from __future__ import annotations

import typing

import absorb


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

    source_dir = absorb.ops.get_source_dir(source, warn=warn)
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
    chunk_size: absorb.ChunkSize | None,
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
        chunk_size=chunk_size,
        filename_template=filename_template,
        table=table,
        source=source,
        parameters=parameters,
        glob=glob,
    )
    return os.path.join(dir_path, filename)


def get_table_filename(
    chunk: absorb.Chunk,
    chunk_size: absorb.ChunkSize | None,
    filename_template: str,
    table: str,
    *,
    source: str | None,
    parameters: dict[str, typing.Any],
    glob: bool = False,
) -> str:
    # gather format parameters
    format_params = parameters.copy()
    if source is not None:
        format_params['source'] = source
    format_params['table'] = table

    # handle chunk formatting
    if '{chunk}' in filename_template:
        if glob:
            if chunk is not None:
                raise Exception('use chunk=None if glob=True')
            format_params['chunk'] = '*'
        else:
            if chunk_size is None:
                raise Exception(
                    'chunk_size must be provided if {chunk} is in filename_template'
                )
            if isinstance(chunk, str):
                chunk_str = chunk
            else:
                if chunk is None:
                    raise Exception('chunk cannot be None')
                chunk_str = absorb.ops.format_chunk(chunk, chunk_size)
            format_params['chunk'] = chunk_str

    # format the filename template
    return filename_template.format(**format_params)
