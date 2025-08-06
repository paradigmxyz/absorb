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
    """print info of either a table or a table recipe"""
    import toolstr

    table = None
    try:
        table = absorb.Table.instantiate(table_str)
    except Exception:
        source, name = table_str.split('.')
        for cls in absorb.ops.get_source_table_classes(source):
            class_name = cls.name_classmethod(allow_generic=True)
            as_camel = absorb.ops.names._camel_to_snake(cls.__qualname__)
            if class_name == name or name == as_camel:
                return print_recipe_info(cls, verbose=verbose)
        else:
            import sys

            print('could not find match')
            sys.exit(1)

    if table is not None:
        absorb.ops.print_table_info(table, verbose=verbose)

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
        'chunk_size',
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
