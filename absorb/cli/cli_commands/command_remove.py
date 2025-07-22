from __future__ import annotations

import typing

import absorb
from .. import cli_outputs
from .. import cli_parsing

if typing.TYPE_CHECKING:
    from argparse import Namespace
    from typing import Any


def remove_command(args: Namespace) -> dict[str, Any]:
    if args.all:
        tracked_datasets = [
            absorb.Table.instantiate(table)
            for table in absorb.ops.get_tracked_tables()
        ]
    else:
        tracked_datasets = cli_parsing._parse_datasets(args)

    if not args.delete_only:
        absorb.ops.stop_tracking_tables(tracked_datasets)
        cli_outputs._print_title('Stopped tracking')
        for dataset in tracked_datasets:
            cli_outputs._print_dataset_bullet(dataset)

    if args.delete:
        import shutil

        print()
        for dataset in tracked_datasets:
            name = dataset.name()
            print('deleting files of ' + name)
            shutil.rmtree(dataset.get_table_dir())
        print('...done')
    else:
        print()
        print('to delete table data files, use the --delete flag')

    return {}
