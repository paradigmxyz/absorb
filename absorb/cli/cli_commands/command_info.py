from __future__ import annotations

import typing
import absorb

from . import command_ls

if typing.TYPE_CHECKING:
    import argparse


def info_command(args: argparse.Namespace) -> dict[str, typing.Any]:
    if args.dataset_or_source is None:
        import sys

        print('specify dataset to print info')
        sys.exit(0)

    if '.' in args.dataset_or_source:
        return print_dataset_info(
            table_str=args.dataset_or_source, verbose=args.verbose
        )
    else:
        return print_source_info(
            source=args.dataset_or_source, verbose=args.verbose
        )


def print_source_info(source: str, verbose: bool) -> dict[str, typing.Any]:
    import toolstr

    toolstr.print_text_box(
        'Data source = ' + source, style='green', text_style='bold white'
    )
    classes = absorb.ops.get_source_table_classes(source)
    if len(classes) == 1:
        print(str(len(classes)), 'table recipe:')
    else:
        print(str(len(classes)), 'table recipes:')
    for cls in classes:
        toolstr.print_bullet(
            key='[white bold]'
            + cls.source
            + '.'
            + cls.name_classmethod(allow_generic=True)
            + '[/white bold]',
            value=str(cls.description),
            **absorb.ops.bullet_styles,
        )

    tracked_datasets = absorb.ops.get_tracked_tables()
    if source is not None:
        tracked_datasets = [
            dataset
            for dataset in tracked_datasets
            if dataset['source_name'] == source
        ]

    print()
    command_ls._print_tracked_datasets(
        tracked_datasets, verbose=verbose, one_per_line=True
    )
    # print()
    command_ls._print_untracked_datasets(
        tracked_datasets,
        verbose=verbose,
        one_per_line=True,
        source=source,
        skip_line=False,
    )

    return {}


def print_dataset_info(table_str: str, verbose: bool) -> dict[str, typing.Any]:
    import toolstr

    try:
        table = absorb.Table.instantiate(table_str)
        print_table_info(table, verbose=verbose)
    except Exception:
        source, name = table_str.split('.')
        for cls in absorb.ops.get_source_table_classes(source):
            if cls.name_classmethod(
                allow_generic=True
            ) == name or name == absorb.ops.names._camel_to_snake(
                cls.__qualname__
            ):
                return print_recipe_info(cls, verbose=verbose)
        else:
            import sys

            print('could not find match')
            sys.exit(1)

    return {}


def print_recipe_info(
    cls: type[absorb.Table], verbose: bool
) -> dict[str, typing.Any]:
    import toolstr

    toolstr.print_text_box(
        'Table recipe = ' + cls.name_classmethod(allow_generic=True),
        style='green',
        text_style='bold white',
    )
    for attr in [
        'description',
        'url',
        'source',
        'write_range',
        'index_type',
    ]:
        if hasattr(cls, attr):
            value = getattr(cls, attr)
        else:
            value = None
        absorb.ops.print_bullet(key=attr, value=value)

    # parameters
    print()
    toolstr.print('[green bold]parameters[/green bold]')
    if len(cls.parameter_types) == 0:
        print('- [none]')
    else:
        for key, value in cls.parameter_types.items():
            if key in cls.default_parameters:
                default = (
                    ' \\[default = ' + str(cls.default_parameters[key]) + ']'
                )
            else:
                default = ''
            absorb.ops.print_bullet(key=key, value=str(value) + default)

    return {}


def print_table_info(
    table: absorb.Table, verbose: bool
) -> dict[str, typing.Any]:
    import toolstr

    schema = table.get_schema()

    toolstr.print_text_box(
        'dataset = ' + table.name(),
        style='green',
        text_style='bold white',
    )

    for attr in [
        'description',
        'url',
        'source',
        'write_range',
        'index_type',
    ]:
        if hasattr(table, attr):
            value = getattr(table, attr)
        else:
            value = None
        absorb.ops.print_bullet(key=attr, value=value)

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
    # absorb.ops.print_bullet(key='tracked', value=table.is_tracked())

    if verbose:
        available_range = table.get_available_range()
        if available_range is not None:
            formatted_available_range = absorb.ops.format_coverage(
                available_range, table.index_type
            )
        else:
            formatted_available_range = 'not available'
        absorb.ops.print_bullet(
            key='available range',
            value=formatted_available_range,
        )

    collected_range = table.get_collected_range()
    absorb.ops.print_bullet(
        key='collected range',
        value=absorb.ops.format_coverage(collected_range, table.index_type),
    )

    import os

    path = table.get_table_dir()
    if os.path.isdir(path):
        bytes_str = absorb.ops.format_bytes(absorb.ops.get_dir_size(path))
    else:
        path = '[not collected]'
        bytes_str = '[not collected]'
    absorb.ops.print_bullet(key='path', value=path)
    absorb.ops.print_bullet(key='size', value=bytes_str)

    return {}
