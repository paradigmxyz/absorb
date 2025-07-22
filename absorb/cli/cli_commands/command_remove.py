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
        print()
        for dataset in tracked_datasets:
            name = dataset.name()
            print('deleting files of ' + name)
            delete_table_dir(dataset, confirm=args.confirm)
        print('...done')
    else:
        print()
        print('to delete table data files, use the --delete flag')

    return {}


def delete_table_dir(table: absorb.Table, confirm: bool = False) -> None:
    import os
    import shutil

    if not confirm:
        raise Exception('use confirm=True to delete table and its data files')

    table_dir = table.get_table_dir()
    if os.path.isdir(table_dir):
        shutil.rmtree(table_dir)

    if absorb.ops.get_config()['use_git']:
        absorb.ops.git_remove_and_commit_file(
            absorb.ops.get_table_metadata_path(table),
            repo_root=absorb.ops.get_absorb_root(),
            message='Remove table metadata for ' + table.name(),
        )
