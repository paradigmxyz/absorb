from __future__ import annotations

import typing

import absorb
from .. import cli_parsing

if typing.TYPE_CHECKING:
    from argparse import Namespace


def download_command(args: Namespace) -> dict[str, typing.Any]:
    # determine tables to download
    tables = cli_parsing._parse_datasets(args)

    # determine bucket to download to
    bucket = cli_parsing._parse_bucket(args)

    # print summary
    print(
        'downloading '
        + str(len(tables))
        + ' tables to bucket '
        + str(bucket['bucket_name'])
    )
    for table in tables:
        print('- ' + table.full_name())

    # exit early if dry
    if args.dry:
        return {}

    # download tables
    absorb.ops.download_tables_to_bucket(tables=tables, bucket=bucket)

    return {}
