from __future__ import annotations

import typing

import absorb
from .. import cli_outputs
from .. import cli_parsing

if typing.TYPE_CHECKING:
    from argparse import Namespace
    from typing import Any


def add_command(args: Namespace) -> dict[str, Any]:
    import json
    import rich

    # parse inputs
    track_datasets = cli_parsing._parse_datasets(args)

    # add untracked collected
    if args.collected:
        track_datasets += [
            absorb.Table.instantiate(table_dict)
            for table_dict in absorb.ops.get_untracked_collected_tables()
        ]

    # filter already collected
    tracked_tables = absorb.ops.get_tracked_tables()
    tracked = [json.dumps(table, sort_keys=True) for table in tracked_tables]
    already_tracked = {}
    not_tracked = {}
    for ds in track_datasets:
        ds_hash = json.dumps(ds.create_table_dict(), sort_keys=True)
        if ds_hash in tracked:
            already_tracked[ds_hash] = ds
        else:
            not_tracked[ds_hash] = ds
    track_datasets = list(not_tracked.values())

    # check for invalid datasets
    sources = set(td.source for td in track_datasets)
    source_datasets = {
        source: absorb.ops.get_source_table_classes(source)
        for source in sources
    }
    for track_dataset in track_datasets:
        if type(track_dataset) not in source_datasets[track_dataset.source]:
            raise Exception('invalid dataset:')

    # print dataset summary
    if len(already_tracked) > 0:
        cli_outputs._print_title('Already tracking')
        for dataset in already_tracked.values():
            cli_outputs._print_dataset_bullet(dataset)
        print()
    cli_outputs._print_title('Now tracking')
    if len(track_datasets) == 0:
        print('[no new datasets specified]')
    else:
        for dataset in track_datasets:
            cli_outputs._print_dataset_bullet(dataset)
        print()
        rich.print(
            'to proceed with data collection, use [white bold]absorb collect[/white bold]'
        )

    # setup directories
    for table in track_datasets:
        table.setup_table_dir()

    # start tracking tables
    absorb.ops.start_tracking_tables(track_datasets)

    # check for missing packages or credentials
    warnings = []
    for table in track_datasets:
        name = table.full_name()

        missing_packages = table.get_missing_packages()
        for package in missing_packages:
            warnings.append(
                f'[red]missing package[/red]: [white]{package}[/white] for [yellow]{name}[/yellow]'
            )

        missing_credentials = table.get_missing_credentials()
        for credential in missing_credentials:
            warnings.append(
                f'[red]missing credentials[/red]: [white]{credential}[/white] for [yellow]{name}[/yellow]'
            )
    if len(warnings) > 0:
        print()
        rich.print('[red]Warnings:[/red]')
        for warning in warnings:
            rich.print('- ' + warning)

    return {}
