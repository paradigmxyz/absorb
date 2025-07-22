from __future__ import annotations

import typing
import absorb

if typing.TYPE_CHECKING:
    import argparse


def info_command(args: argparse.Namespace) -> dict[str, typing.Any]:
    import toolstr

    if args.dataset is None:
        import sys

        print('specify dataset to print info')
        sys.exit(0)

    table = absorb.Table.instantiate(args.dataset)
    schema = table.get_schema()

    toolstr.print_text_box(
        'dataset = ' + args.dataset, style='green', text_style='bold white'
    )

    # metadata
    if table.description is not None:
        absorb.ops.print_bullet(
            key='description',
            value=table.description,
        )

    for attr in [
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
    available_range = table.get_available_range()
    if available_range is not None:
        formatted_available_range = absorb.ops.format_coverage(
            available_range, table.index_type
        )
    else:
        formatted_available_range = 'not available'
    collected_range = table.get_collected_range()
    absorb.ops.print_bullet(
        key='available range',
        value=formatted_available_range,
    )
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
