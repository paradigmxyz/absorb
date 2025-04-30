from __future__ import annotations

import typing

import truck
from . import cli_helpers

if typing.TYPE_CHECKING:
    import argparse


def get_subcommands() -> (
    list[tuple[str, str, list[tuple[list[str], dict[str, typing.Any]]]]]
):
    return [
        (
            'ls',
            'list tracked datasets',
            [
                (
                    ['--verbose', '-v'],
                    {
                        'action': 'store_true',
                        'help': 'show verbose details',
                    },
                ),
            ],
        ),
        (
            'collect',
            'collect datasets',
            [
                (
                    ['dataset'],
                    {
                        'nargs': '*',
                        'help': 'dataset to track, format as "<source>.<dataset>"',
                    },
                ),
                (
                    ['--parameters'],
                    {'nargs': '*', 'help': 'dataset parameters'},
                ),
                (
                    ['--dry'],
                    {
                        'action': 'store_true',
                        'help': 'perform dry run (avoids collecting data)',
                    },
                ),
                (
                    ['--overwrite'],
                    {
                        'action': 'store_true',
                        'help': 'overwrite existing files',
                    },
                ),
                (
                    ['--range'],
                    {
                        'help': 'range of data to collect',
                    },
                ),
                (
                    ['-v', '--verbose'],
                    {
                        'help': 'display extra information',
                        'nargs': '?',
                        'const': 1,
                        'default': 1,
                        'type': int,
                    },
                ),
            ],
        ),
        (
            'add',
            'start tracking datasets',
            [
                (
                    ['dataset'],
                    {
                        'nargs': '*',
                        'help': 'dataset to track, format as "<source>.<dataset>"',
                    },
                ),
                (
                    ['--parameters'],
                    {'nargs': '*', 'help': 'dataset parameters'},
                ),
                (
                    ['--all'],
                    {
                        'help': 'add all available datasets',
                        'action': 'store_true',
                    },
                ),
                (
                    ['--path'],
                    {'help': 'directory location to store the dataset'},
                ),
            ],
        ),
        (
            'remove',
            'remove tracking datasets',
            [
                (
                    ['dataset'],
                    {
                        'nargs': '*',
                        'help': 'dataset to track, format as "<source>.<dataset>"',
                    },
                ),
                (
                    ['--parameters'],
                    {'nargs': '*', 'help': 'dataset parameters'},
                ),
                (
                    ['--all'],
                    {
                        'help': 'add all available datasets',
                        'action': 'store_true',
                    },
                ),
            ],
        ),
        (
            'path',
            'print truck root path or dataset path',
            [
                (
                    ['dataset'],
                    {
                        'nargs': '?',
                        'help': 'dataset to track, format as "<source>.<dataset>"',
                    },
                ),
                (
                    ['--parameters'],
                    {'nargs': '*', 'help': 'dataset parameters'},
                ),
                (
                    ['--glob'],
                    {'action': 'store_true'},
                ),
            ],
        ),
    ]


def parse_args() -> argparse.Namespace:
    import argparse
    import importlib

    parser = argparse.ArgumentParser(
        formatter_class=cli_helpers.HelpFormatter, allow_abbrev=False
    )
    subparsers = parser.add_subparsers(dest='command')

    parsers = {}
    for name, description, arg_args in get_subcommands():
        f_module = importlib.import_module(
            'truck.cli.cli_commands.command_' + name
        )
        f = getattr(f_module, name + '_command')
        subparser = subparsers.add_parser(name, help=description)
        subparser.set_defaults(f_command=f)
        for sub_args, sub_kwargs in arg_args:
            subparser.add_argument(*sub_args, **sub_kwargs)
        subparser.add_argument(
            '--debug',
            '--pdb',
            help='enter debugger upon error',
            action='store_true',
        )
        subparser.add_argument(
            '-i',
            '--interactive',
            help='open data in interactive python session',
            action='store_true',
        )
        parsers[name] = subparser

    # parse args
    args = parser.parse_args()

    # display help if no command specified
    if args.command is None:
        import sys

        parser.print_help()
        sys.exit(0)

    return args


def _parse_datasets(args: argparse.Namespace) -> list[truck.TrackedTable]:
    # parse parameters
    parameters: dict[str, typing.Any] = {}
    if args.parameters is not None:
        for parameter in args.parameters:
            key, value = parameter.split('=')
            parameters[key] = value

    # parse datasets
    sources = []
    tables = []
    for dataset in args.dataset:
        if '.' in dataset:
            source, table = dataset.split('.')
            sources.append(source)
            tables.append(table)
        else:
            for source_dataset in truck.get_source_tables(dataset):
                sources.append(dataset)
                tables.append(source_dataset.__name__)

    parsed = []
    for source, table in zip(sources, tables):
        camel_table = truck.ops.names._snake_to_camel(table)
        tracked_table: truck.TrackedTable = {
            'source_name': source,
            'table_name': table,
            'table_class': 'truck.datasets.' + source + '.' + camel_table,
            'parameters': parameters,
        }
        parsed.append(tracked_table)
    return parsed


def _parse_ranges(
    raw_ranges: list[str] | None, range_format: truck.RangeFormat
) -> list[typing.Any] | None:
    """
    examples:
    --range 2025-01-01:2025-03-01
    --range 2025-01-01:
    --range :2025-01-01
    """
    if raw_ranges is None:
        return None
    if range_format == 'date' or range_format == 'date_range':
        raise NotImplementedError('manual ranges for ' + str(range_format))
    else:
        raise NotImplementedError('manual ranges for ' + str(range_format))

    # 'date',
    # 'date_range',
    # 'named_range',
    # 'block_range',
    # 'id_range',
    # 'count',
    # None,
