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
            'show info about a specific dataset or data source',
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
                        'nargs': '?',
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


def parse_args() -> argparse.Namespace:
    import argparse
    import importlib

    parser = argparse.ArgumentParser(
        formatter_class=cli_helpers.HelpFormatter, allow_abbrev=False
    )
    parser.add_argument(
        '--cd-destination-tempfile',
        help=argparse.SUPPRESS,
    )
    subparsers = parser.add_subparsers(dest='command')

    parsers = {}
    for name, description, arg_args in get_subcommands():
        f_module = importlib.import_module(
            'absorb.cli.cli_commands.command_' + name
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
        subparser.add_argument(
            '--cd-destination-tempfile',
            help=argparse.SUPPRESS,
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


def _parse_datasets(args: argparse.Namespace) -> list[absorb.TableDict]:
    # parse datasets
    sources = []
    tables = []
    if isinstance(args.dataset, list):
        datasets = args.dataset
    elif isinstance(args.dataset, str):
        datasets = [args.dataset]
    else:
        raise Exception()
    for dataset in datasets:
        # get source and table
        if '.' in dataset:
            source, table = dataset.split('.')
            sources.append(source)
            tables.append(table)
        else:
            for source_dataset in absorb.ops.get_source_tables(dataset):
                sources.append(dataset)
                tables.append(source_dataset.__name__)

    # create TableDict dicts
    parsed = []
    for source, table in zip(sources, tables):
        table_class = absorb.ops.get_table_class(
            source=source, table_name=table
        )
        parameters = _parse_parameters(
            table_class, args.parameters, use_all=len(tables) == 1
        )
        tracked_table: absorb.TableDict = {
            'source_name': source,
            'table_name': table_class.class_name(parameters=parameters),
            'table_class': table_class.full_class_name(),
            'parameters': parameters,
        }
        parsed.append(tracked_table)

    return parsed


def _parse_parameters(
    table_class: type[absorb.Table],
    raw_parameters: list[str] | None,
    use_all: bool = True,
) -> dict[str, typing.Any]:
    # parse parameters
    parameters: dict[str, typing.Any] = {}
    value: typing.Any
    if raw_parameters is not None:
        for parameter in raw_parameters:
            key, value = parameter.split('=')

            # set parameter type
            if key not in table_class.parameter_types:
                if use_all:
                    raise Exception(
                        'unknown parameter: '
                        + str(key)
                        + ' not in '
                        + str(list(parameters.keys()))
                    )
                else:
                    continue
            parameter_type = table_class.parameter_types[key]
            if parameter_type == str:  # noqa: E721
                pass
            elif parameter_type == int:  # noqa: E721
                value = int(value)
            elif parameter_type == list[str]:
                value = value.split(',')
            elif parameter_type == list[int]:
                value = [int(subvalue) for subvalue in value.split(',')]
            else:
                raise Exception(
                    'invalid parameter type: ' + str(parameter_type)
                )

            parameters[key] = value

    return parameters


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
