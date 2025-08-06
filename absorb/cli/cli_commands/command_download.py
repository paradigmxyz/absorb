from __future__ import annotations

import typing

import absorb
from .. import cli_parsing

if typing.TYPE_CHECKING:
    from argparse import Namespace


def download_command(args: Namespace) -> dict[str, typing.Any]:
    import toolstr

    # determine tables to download
    tables = cli_parsing._parse_datasets(args)

    # determine bucket to download to
    bucket = cli_parsing._parse_bucket(args)

    # print summary
    if len(tables) == 1:
        word = 'table'
    else:
        word = 'tables'
    open_tag = '[green bold]'
    close_tag = '[/green bold]'
    toolstr.print(
        'downloading '
        + open_tag
        + str(len(tables))
        + close_tag
        + ' '
        + word
        + ' to bucket '
        + open_tag
        + str(bucket['bucket_name'])
        + close_tag
    )
    if len(tables) > 1:
        for table in tables:
            toolstr.print_bullet(value=table.full_name(), key=None, indent=4)
        print()

    # exit early if dry
    if args.dry:
        return {}

    # download tables
    for table in tables:
        absorb.ops.download(table=table, bucket=bucket)

    return {}
