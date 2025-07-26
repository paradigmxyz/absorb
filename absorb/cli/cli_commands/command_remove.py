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
        absorb.ops.remove(tracked_datasets)
        cli_outputs._print_title('Stopped tracking')
        for dataset in tracked_datasets:
            cli_outputs._print_dataset_bullet(dataset)

    if args.delete or args.delete_only:
        print()
        for dataset in tracked_datasets:
            print('deleting files of ' + dataset.full_name())
            try:
                if args.delete_only:
                    absorb.ops.delete_table_data(dataset, confirm=args.confirm)
                else:
                    absorb.ops.delete_table_dir(dataset, confirm=args.confirm)
            except absorb.ConfirmError:
                import sys
                import toolstr

                toolstr.print('[red]use --confirm to delete files[/red]')
                sys.exit(0)
        print('...done')
    else:
        print()
        print('to delete table data files, use the --delete flag')

    return {}
