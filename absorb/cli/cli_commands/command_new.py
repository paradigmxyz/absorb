from __future__ import annotations

import typing

import absorb

if typing.TYPE_CHECKING:
    from argparse import Namespace
    from typing import Any


def new_command(args: Namespace) -> dict[str, Any]:
    import os
    import subprocess

    # parse names
    source_name, table_name = args.dataset.split('.')
    snake_table_name = absorb.ops.names._camel_to_snake(table_name)
    camel_table_name = absorb.ops.names._snake_to_camel(table_name)

    # create template content
    table_template = get_table_class_template()
    table_content = table_template.replace(
        '{source_name}', source_name
    ).replace('{class_name}', camel_table_name)

    # determine path
    if args.path is not None:
        path = args.path
    elif args.native:
        source_path = os.path.join(absorb.__path__[0], 'datasets', source_name)
        if os.path.isdir(source_path):
            path = os.path.join(source_path, snake_table_name + '.py')
        else:
            path = os.path.join(source_path + '.py')
    else:
        raise NotImplementedError(
            'creating table definitions in ABSORB_ROOT, use --native or --path'
        )
    print('adding table to file: ' + path)

    # edit file
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if os.path.isfile(path):
        content = '\n\n' + table_content
        with open(path, 'a') as f:
            f.write(content)
    else:
        content = get_data_source_template() + '\n\n\n' + table_content
        with open(path, 'w') as f:
            f.write(content)

    # edit file if EDITOR set
    editor = get_editor()
    if editor is not None:
        subprocess.call([editor, path])
    else:
        print(
            '(not opening editor because EDITOR environment variable not set)'
        )

    return {}


def get_data_source_template() -> str:
    return """from __future__ import annotations

import typing

import absorb

if typing.TYPE_CHECKING:
    import polars as pl"""


def get_table_class_template() -> str:
    template = """class {class_name}(absorb.Table):
    source = '{source_name}'
    write_range = 'overwrite_all'
    parameter_types = {}

    def get_schema(self) -> dict[str, pl.DataType | type[pl.DataType]]:
        import polars as pl

        return {}

    def collect_chunk(self, chunk: absorb.Chunk) -> pl.DataFrame | None:
        raise NotImplementedError()

    def get_available_range(self) -> absorb.Coverage:
        raise NotImplementedError()
"""
    return template


def get_editor() -> str | None:
    import os

    return os.environ.get('EDITOR')
