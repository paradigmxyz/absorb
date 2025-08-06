from __future__ import annotations

import typing

import absorb
from . import cli_helpers

if typing.TYPE_CHECKING:
    import argparse
    import datetime


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
                    ['-v', '--verbose'],
                    {
                        'help': 'display extra information',
                        'nargs': '?',
                        'const': 1,
                        'default': 0,
                        'type': int,
                    },
                ),
            ],
        ),
        (
            'info',
            'show info about a specific dataset or source',
            [
                (
                    ['dataset_or_source'],
                    {
                        'help': 'dataset or data source',
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
                        'nargs': '+',
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
                    {
                        'nargs': '*',
                        'help': 'dataset parameters as `key=value` args',
                        'metavar': 'PARAMS',
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
                        'help': 'add all datasets that are already collected',
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
                (
                    ['--confirm'],
                    {
                        'action': 'store_true',
                        'help': 'confirm the deletion of dataset files',
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
                    {
                        'nargs': '*',
                        'help': 'dataset parameters',
                        'metavar': 'PARAMS',
                    },
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
                    {
                        'nargs': '*',
                        'help': 'dataset parameters',
                        'metavar': 'PARAMS',
                    },
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
                    {
                        'nargs': '*',
                        'help': 'dataset parameters',
                        'metavar': 'PARAMS',
                    },
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
            'sql',
            'run SQL query',
            [
                (
                    ['sql'],
                    {
                        'help': 'SQL query to run',
                    },
                ),
                (
                    ['--backend'],
                    {
                        'default': 'absorb',
                        'help': 'SQL backend to use {absorb, dune, snowflake}',
                    },
                ),
                (
                    ['--output-file'],
                    {
                        'help': 'parquet file to save query results',
                        'nargs': '?',
                        'metavar': 'FILE',
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
                (
                    ['--enable-git'],
                    {
                        'action': 'store_true',
                        'help': 'enable git tracking for config and metadata',
                    },
                ),
                (
                    ['--disable-git'],
                    {
                        'action': 'store_true',
                        'help': 'disable git tracking for config and metadata',
                    },
                ),
                (
                    ['--set-default-bucket'],
                    {
                        'help': 'set default bucket using json blob',
                        'metavar': 'JSON',
                    },
                ),
                (
                    ['--set-default-rclone-remote'],
                    {
                        'help': 'set rclone remote to use for bucket uploads and downloads',
                        'metavar': 'REMOTE',
                    },
                ),
                (
                    ['--set-default-bucket-name'],
                    {
                        'help': 'set default bucket name for upload and downloads',
                        'metavar': 'BUCKET',
                    },
                ),
                (
                    ['--set-default-provider'],
                    {
                        'help': 'set default bucket provider for upload and downloads',
                        'metavar': 'PROVIDER',
                    },
                ),
                (
                    ['--set-default-path-prefix'],
                    {
                        'help': 'set default path prefix for bucket paths',
                        'metavar': 'PREFIX',
                    },
                ),
                (
                    ['--clear-default-bucket'],
                    {
                        'help': 'clear default bucket setting',
                        'action': 'store_true',
                    },
                ),
                (
                    ['--clear-default-rclone-remote'],
                    {
                        'help': 'clear rclone remote to use for bucket uploads and downloads',
                        'action': 'store_true',
                    },
                ),
                (
                    ['--clear-default-bucket-name'],
                    {
                        'help': 'clear bucket name to use for upload and downloads',
                        'action': 'store_true',
                    },
                ),
                (
                    ['--clear-default-provider'],
                    {
                        'help': 'clear provider to use for upload and downloads',
                        'action': 'store_true',
                    },
                ),
                (
                    ['--clear-default-path-prefix'],
                    {
                        'help': 'clear default path prefix for bucket paths',
                        'action': 'store_true',
                    },
                ),
                (
                    ['-v', '--verbose'],
                    {
                        'action': 'store_true',
                        'help': 'display extra information',
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
                    {
                        'nargs': '*',
                        'help': 'dataset parameters',
                        'metavar': 'PARAMS',
                    },
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
        (
            'upload',
            'upload datasets to a cloud bucket',
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
                        'metavar': 'PARAMS',
                    },
                ),
                (
                    ['--rclone-remote'],
                    {
                        'help': 'name of rclone remote to use',
                        'metavar': 'REMOTE',
                    },
                ),
                (
                    ['--bucket'],
                    {
                        'help': 'name of bucket to upload to',
                        'metavar': 'BUCKET',
                    },
                ),
                (
                    ['--provider'],
                    {
                        'help': 'bucket provider (e.g. gcp, aws, azure)',
                    },
                ),
                (
                    ['--path-prefix'],
                    {
                        'help': 'path prefix to use for the bucket',
                        'metavar': 'PREFIX',
                    },
                ),
                (
                    ['--dry'],
                    {
                        'action': 'store_true',
                        'help': 'perform dry run (avoids uploading data)',
                    },
                ),
            ],
        ),
        (
            'download',
            'download datasets to a cloud bucket',
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
                        'metavar': 'PARAMS',
                    },
                ),
                (
                    ['--rclone-remote'],
                    {
                        'help': 'name of rclone remote to use',
                        'metavar': 'REMOTE',
                    },
                ),
                (
                    ['--bucket'],
                    {
                        'help': 'name of bucket to download to',
                        'metavar': 'BUCKET',
                    },
                ),
                (
                    ['--provider'],
                    {
                        'help': 'bucket provider (e.g. gcp, aws, azure)',
                    },
                ),
                (
                    ['--path-prefix'],
                    {
                        'help': 'path prefix to use for the bucket',
                        'metavar': 'PREFIX',
                    },
                ),
                (
                    ['--dry'],
                    {
                        'action': 'store_true',
                        'help': 'perform dry run (avoids downloading data)',
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
                # 'help': 'open data in interactive python session',
                'help': argparse.SUPPRESS,
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
    raw_ranges: list[str] | None,
) -> list[tuple[datetime.datetime | None, datetime.datetime | None]] | None:
    """
    range formats:
    - `START:END` cover start to end
    - `START` cover start only
    - `START:` cover start to now
    - `:END` cover beginning to end
    - `:` cover entire range

    START / END formats:
    - year (2024)
    - month (2024-03)
    - day (2024-01-05)

    examples:
    --range 2025-01-01:2025-03-01
    --range 2025-01-01:
    --range :2025-01-01
    """
    import datetime

    if raw_ranges is None:
        return None

    output: list[tuple[datetime.datetime | None, datetime.datetime | None]] = []
    for raw_range in raw_ranges:
        if ':' not in raw_range:
            start = absorb.ops.parse_raw_datetime(raw_range)
            if absorb.ops.is_year(raw_range):
                end = datetime.datetime(int(raw_range) + 1, 1, 1)
            elif absorb.ops.is_month(raw_range):
                if start.month == 12:
                    end = datetime.datetime(start.year + 1, 1, 1)
                else:
                    end = datetime.datetime(start.year, start.month + 1, 1)
            elif absorb.ops.is_day(raw_range):
                end = start + datetime.timedelta(days=1)
            else:
                raise ValueError('Invalid range format: ' + str(raw_range))
        else:
            parts = raw_range.split(':')
            if len(parts) == 1:
                timestamp = absorb.ops.parse_raw_datetime(parts[0])
                if raw_range.startswith(':'):
                    output.append((None, timestamp))
                elif raw_range.endswith(':'):
                    output.append((timestamp, None))
                else:
                    raise Exception()
            elif len(parts) == 2:
                if parts[0] == '':
                    start = None
                else:
                    start = absorb.ops.parse_raw_datetime(parts[0])
                if parts[1] == '':
                    end = None
                else:
                    end = absorb.ops.parse_raw_datetime(parts[1])
            else:
                raise ValueError('Invalid range format: ' + str(raw_range))
        output.append((start, end))
    return output


def _parse_bucket(args: argparse.Namespace) -> absorb.Bucket:
    default_bucket = absorb.ops.get_config()['default_bucket']
    if args.rclone_remote is not None:
        rclone_remote = args.rclone_remote
    else:
        rclone_remote = default_bucket['rclone_remote']
    if args.path_prefix is not None:
        path_prefix = args.path_prefix
    else:
        path_prefix = default_bucket['path_prefix']
    if args.bucket is not None:
        bucket_name = args.bucket
    else:
        bucket_name = default_bucket['bucket_name']
    if args.provider is not None:
        provider = args.provider
    else:
        provider = default_bucket['provider']
    return {
        'rclone_remote': rclone_remote,
        'bucket_name': bucket_name,
        'path_prefix': path_prefix,
        'provider': provider,
    }
