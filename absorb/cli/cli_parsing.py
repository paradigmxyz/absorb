from __future__ import annotations

import typing

import absorb
from . import cli_helpers

if typing.TYPE_CHECKING:
    import argparse


def get_subcommands() -> list[
    tuple[str, str, list[tuple[list[str], dict[str, typing.Any]]]]
]:
    return [
        (
            'ls',
            'list tracked datasets',
            [
                (
                    ['source'],
                    {
                        'nargs': '?',
                        'help': 'data source',
                    },
                ),
                (
                    ['--available'],
                    {
                        'action': 'store_true',
                        'help': 'list available datasets',
                    },
                ),
                (
                    ['--tracked'],
                    {
                        'action': 'store_true',
                        'help': 'list tracked datasets',
                    },
                ),
                (
                    ['--untracked-collected'],
                    {
                        'action': 'store_true',
                        'help': 'list untracked collected datasets',
                    },
                ),
                (
                    ['--one-per-line', '-1'],
                    {
                        'action': 'store_true',
                        'help': 'list one dataset per line',
                    },
                ),
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
            'info',
            'show info about a specific dataset or source',
            [
                (
                    ['dataset'],
                    {
                        'help': 'dataset or data source',
                    },
                )
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
                    ['--setup-only'],
                    {
                        'action': 'store_true',
                        'help': 'only setup the table directory, do not collect data',
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
                    ['--path'],
                    {'help': 'directory location to store the dataset'},
                ),
                (
                    ['--collected'],
                    {
                        'action': 'store_true',
                        'help': 'add datasets that are already collected',
                    },
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
                    {
                        'nargs': '*',
                        'help': 'dataset parameters',
                        'metavar': 'PARAM=VALUE',
                    },
                ),
                (
                    ['--all'],
                    {
                        'help': 'add all available datasets',
                        'action': 'store_true',
                    },
                ),
                (
                    ['--delete'],
                    {
                        'action': 'store_true',
                        'help': 'delete the dataset files from disk',
                    },
                ),
                (
                    ['--delete-only'],
                    {
                        'action': 'store_true',
                        'help': 'keep tracking table, but delete the dataset files from disk',
                    },
                ),
            ],
        ),
        (
            'path',
            'print absorb root path or dataset path',
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
        (
            'cd',
            'change directory to an absorb path',
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
        (
            'new',
            'create new dataset',
            [
                (
                    ['dataset'],
                    {
                        'nargs': '?',
                        'help': 'dataset to track, format as "<source>.<dataset>"',
                    },
                ),
                (
                    ['--path'],
                    {
                        'help': 'path where to store new table definition',
                    },
                ),
                (
                    ['--native'],
                    {
                        'action': 'store_true',
                        'help': 'create definition directly in absorb repo',
                    },
                ),
            ],
        ),
        (
            'preview',
            'preview rows of a dataset',
            [
                (
                    ['dataset'],
                    {
                        'nargs': '+',
                        'help': 'dataset to track, format as "<source>.<dataset>"',
                    },
                ),
                (
                    ['--parameters'],
                    {'nargs': '*', 'help': 'dataset parameters'},
                ),
                (
                    ['--count'],
                    {
                        'type': int,
                        'default': 10,
                        'help': 'number of rows to preview',
                    },
                ),
                (
                    ['--offset'],
                    {
                        'type': int,
                        'default': 0,
                        'help': 'number of rows to preview',
                    },
                ),
            ],
        ),
        (
            'setup',
            'setup environment',
            [
                (
                    ['dataset'],
                    {
                        'nargs': '*',
                        'help': 'dataset to track, format as "<source>.<dataset>"',
                    },
                ),
                (
                    ['--regenerate-metadata'],
                    {
                        'action': 'store_true',
                        'help': 'regenerate metadata for dataset(s)',
                    },
                ),
                (
                    ['--regenerate-config'],
                    {
                        'action': 'store_true',
                        'help': 'regenerate configuration, preserving as many settings as possible',
                    },
                ),
            ],
        ),
        (
            'validate',
            'validate datasets',
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
                    ['--verbose', '-v'],
                    {
                        'action': 'store_true',
                        'help': 'display extra information',
                    },
                ),
            ],
        ),
    ]


def get_common_args() -> list[tuple[list[str], dict[str, typing.Any]]]:
    import argparse

    return [
        (
            [
                '--debug',
                '--pdb',
            ],
            {
                'help': 'enter debugger upon error',
                'action': 'store_true',
            },
        ),
        (
            [
                '-i',
                '--interactive',
            ],
            {
                'help': 'open data in interactive python session',
                'action': 'store_true',
            },
        ),
        (
            [
                '--absorb-root',
            ],
            {
                'help': 'path to absorb root directory',
                'metavar': 'PATH',
            },
        ),
        (
            [
                '--cd-destination-tempfile',
            ],
            {
                'help': argparse.SUPPRESS,
            },
        ),
    ]


def parse_args() -> argparse.Namespace:
    """parse input arguments into a Namespace object"""
    import argparse
    import importlib
    import sys

    # create top-level parser
    parser = argparse.ArgumentParser(
        formatter_class=cli_helpers.HelpFormatter, allow_abbrev=False
    )
    parser.add_argument('--cd-destination-tempfile', help=argparse.SUPPRESS)

    # create subparsers
    subparsers = parser.add_subparsers(dest='command')
    common_args = get_common_args()
    for name, description, arg_args in get_subcommands():
        module_name = 'absorb.cli.cli_commands.command_' + name
        f_module = importlib.import_module(module_name)
        subparser = subparsers.add_parser(name, help=description)
        subparser.set_defaults(f_command=getattr(f_module, name + '_command'))
        for sub_args, sub_kwargs in arg_args + common_args:
            subparser.add_argument(*sub_args, **sub_kwargs)

    # parse args
    args = parser.parse_args()

    # display help if no command specified
    if args.command is None:
        parser.print_help()
        sys.exit(0)

    return args


def _parse_datasets(args: argparse.Namespace) -> list[absorb.Table]:
    """parse the datasets parameter into a list of instantiated Tables"""
    # parse parameters
    parameters: dict[str, typing.Any] = {}
    if args.parameters is not None:
        for parameter in args.parameters:
            key, value = parameter.split('=')
            parameters[key] = value

    # parse tables
    tables = []
    for table_str in args.dataset:
        table = absorb.Table.instantiate(table_str, raw_parameters=parameters)
        tables.append(table)

    return tables


def _parse_ranges(
    raw_ranges: list[str] | None, index_type: absorb.IndexType
) -> list[typing.Any] | None:
    """
    examples:
    --range 2025-01-01:2025-03-01
    --range 2025-01-01:
    --range :2025-01-01
    """
    if raw_ranges is None:
        return None
    if index_type == 'day' or index_type == 'timestamp_range':
        raise NotImplementedError('manual ranges for ' + str(index_type))
    else:
        raise NotImplementedError('manual ranges for ' + str(index_type))

    # 'date',
    # 'date_range',
    # 'named_range',
    # 'block_range',
    # 'id_range',
    # 'count',
    # None,
