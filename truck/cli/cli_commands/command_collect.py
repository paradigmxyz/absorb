from __future__ import annotations

import typing

import truck
from .. import cli_parsing

if typing.TYPE_CHECKING:
    from argparse import Namespace
    from typing import Any


def collect_command(args: Namespace) -> dict[str, Any]:
    if len(args.dataset) > 0:
        datasets = cli_parsing._parse_datasets(args)
    else:
        datasets = truck.ops.config.get_tracked_tables()

    for dataset in datasets:
        instance = truck.Table.instantiate(dataset)
        if instance.write_range == 'append_only':
            data_ranges = cli_parsing._parse_ranges(
                args.range, range_format=instance.range_format
            )
        else:
            data_ranges = None
        instance.collect(data_range=data_ranges)

    return {}
