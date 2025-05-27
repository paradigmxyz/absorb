from __future__ import annotations

import typing

import absorb
from .. import cli_parsing

if typing.TYPE_CHECKING:
    from argparse import Namespace
    from typing import Any


def collect_command(args: Namespace) -> dict[str, Any]:
    if len(args.dataset) > 0:
        datasets = cli_parsing._parse_datasets(args)
    else:
        datasets = absorb.ops.config.get_tracked_tables()

    first = True
    for dataset in datasets:
        if not first:
            print()
        instance = absorb.Table.instantiate(dataset)
        if instance.write_range == 'append_only':
            data_ranges = cli_parsing._parse_ranges(
                args.range, index_type=instance.index_type
            )
        else:
            data_ranges = None
        instance.collect(
            data_range=data_ranges,
            dry=args.dry,
            overwrite=args.overwrite,
            verbose=args.verbose,
        )
        first = False

    return {}
