from __future__ import annotations

import typing

import truck
from .. import cli_outputs
from .. import cli_parsing

if typing.TYPE_CHECKING:
    from argparse import Namespace
    from typing import Any


def add_command(args: Namespace) -> dict[str, Any]:
    import json
    import rich

    # parse inputs
    if args.all:
        track_datasets = truck.ops.catalog.get_available_tables()
    else:
        track_datasets = cli_parsing._parse_datasets(args)

    # use snake case throughout
    for track_dataset in track_datasets:
        track_dataset['source_name'] = truck.ops.names._camel_to_snake(
            track_dataset['source_name']
        )
        track_dataset['table_name'] = truck.ops.names._camel_to_snake(
            track_dataset['table_name']
        )

    # filter already collected
    tracked = [
        json.dumps(table, sort_keys=True)
        for table in truck.get_tracked_tables()
    ]
    already_tracked = []
    not_tracked = []
    for ds in track_datasets:
        if json.dumps(ds, sort_keys=True) in tracked:
            already_tracked.append(ds)
        else:
            not_tracked.append(ds)
    track_datasets = not_tracked

    # check for invalid datasets
    sources = set(td['source_name'] for td in track_datasets)
    source_datasets = {
        source: [
            table.class_name() for table in truck.get_source_tables(source)
        ]
        for source in sources
    }
    for track_dataset in track_datasets:
        if (
            track_dataset['table_name']
            not in source_datasets[track_dataset['source_name']]
        ):
            raise Exception('invalid dataset:')

    # print dataset summary
    if len(already_tracked) > 0:
        cli_outputs._print_title('Already tracking')
        for dataset in already_tracked:
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
            'to proceed with data collection, use [white bold]truck collect[/white bold]'
        )
    truck.start_tracking_tables(track_datasets)

    return {}
