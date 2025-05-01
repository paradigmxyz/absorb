from __future__ import annotations

import typing

import truck
from .. import cli_outputs
from .. import cli_parsing

if typing.TYPE_CHECKING:
    from argparse import Namespace
    from typing import Any


def remove_command(args: Namespace) -> dict[str, Any]:
    if args.all:
        tracked_datasets = truck.ops.get_tracked_tables()
    else:
        tracked_datasets = cli_parsing._parse_datasets(args)
    truck.ops.stop_tracking_tables(tracked_datasets)
    cli_outputs._print_title('Stopped tracking')
    for dataset in tracked_datasets:
        cli_outputs._print_dataset_bullet(dataset)
    return {}
