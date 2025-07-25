from __future__ import annotations

import typing

import absorb
from .. import cli_parsing

if typing.TYPE_CHECKING:
    from argparse import Namespace


def upload_command(args: Namespace) -> dict[str, typing.Any]:
    # determine tables to upload
    tables = cli_parsing._parse_datasets(args)

    # determine bucket to upload to
    bucket = cli_parsing._parse_bucket(args)

    # print summary
    print(
        'uploading '
        + str(len(tables))
        + ' tables to bucket '
        + str(bucket['bucket_name'])
    )
    for table in tables:
        print('- ' + table.full_name())

    # exit early if dry
    if args.dry:
        return {}

    # upload tables
    absorb.ops.upload_tables_to_bucket(tables=tables, bucket=bucket)

    return {}
