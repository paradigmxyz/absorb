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

    table = absorb.ops.resolve_table(args.dataset)
    schema = table.get_schema()

    toolstr.print_text_box(
        'dataset = ' + args.dataset, style='green', text_style='bold white'
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
    print()
    toolstr.print('[green bold]parameters[/green bold]')
    if table.parameters is None or len(table.parameter_types) == 0:
        print('- [none]')
    else:
        for key, value in table.parameter_types.items():
            if key in table.default_parameters:
                default = (
                    ' \[default = ' + str(table.default_parameters[key]) + ']'
                )
            else:
                default = ''
            absorb.ops.print_bullet(key=key, value=str(value) + default)
    print()
    toolstr.print('[green bold]schema[/green bold]')
    for key, value in schema.items():
        absorb.ops.print_bullet(key=key, value=value)

    return {}
