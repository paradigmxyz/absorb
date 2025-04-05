from __future__ import annotations

import typing

import truck
from . import cli_outputs
from . import cli_parsing

if typing.TYPE_CHECKING:
    from argparse import Namespace
    from typing import Any


def get_subcommands() -> (
    list[
        tuple[
            str,
            str,
            typing.Callable[[Namespace], dict[str, Any]],
            list[tuple[list[str], dict[str, Any]]],
        ]
    ]
):
    return [
        (
            'ls',
            'list tracked datasets',
            ls_command,
            [],
        ),
        (
            'collect',
            'collect datasets',
            collect_command,
            [],
        ),
        (
            'track',
            'start tracking datasets',
            track_command,
            [
                (
                    ['dataset'],
                    {
                        'nargs': '*',
                        'help': 'dataset to track, format as "<source>.<dataset>"',
                    },
                ),
                (
                    ['--path'],
                    {'help': 'directory location to store the dataset'},
                ),
                (
                    ['--parameters'],
                    {'nargs': '*', 'help': 'dataset parameters'},
                ),
            ],
        ),
        (
            'stop',
            'stop tracking datasets',
            stop_command,
            [
                (
                    ['dataset'],
                    {
                        'nargs': '*',
                        'help': 'dataset to track, format as "<source>.<dataset>"',
                    },
                ),
                (
                    ['--parameters'],
                    {'nargs': '*', 'help': 'dataset parameters'},
                ),
            ],
        ),
        (
            'path',
            'print truck root path or dataset path',
            path_command,
            [
                (
                    ['dataset'],
                    {
                        'nargs': '?',
                        'help': 'dataset to track, format as "<source>.<dataset>"',
                    },
                ),
                (
                    ['--parameters'],
                    {'nargs': '*', 'help': 'dataset parameters'},
                ),
                (
                    ['--glob'],
                    {'action': 'store_true'},
                ),
            ],
        ),
    ]


def path_command(args: Namespace) -> dict[str, Any]:
    if args.dataset is None:
        print(truck.get_truck_root(warn=False))
    elif args.glob:
        print(truck.get_table_glob(args.dataset, warn=False))
    elif '.' in args.dataset:
        source, table = args.dataset.split('.')
        print(truck.get_table_dir(args.dataset, warn=False))
    else:
        print(truck.get_source_dir(args.dataset, warn=False))
    return {}


def stop_command(args: Namespace) -> dict[str, Any]:
    tracked_datasets = cli_parsing._parse_datasets(args)
    truck.stop_tracking_tables(tracked_datasets)
    cli_outputs._print_title('Stopped tracking')
    for dataset in tracked_datasets:
        cli_outputs._print_dataset_bullet(dataset)
    return {}


def ls_command(args: Namespace) -> dict[str, Any]:
    import truck

    tracked_datasets = truck.get_tracked_tables()

    cli_outputs._print_title('Available datasets')
    for source in truck.get_sources():
        cli_outputs._print_source_datasets_bullet(
            source, truck.get_source_tables(source)
        )
    print()
    cli_outputs._print_title('Tracked datasets')
    if len(tracked_datasets) == 0:
        print('[none]')
    else:
        for dataset in tracked_datasets:
            cli_outputs._print_dataset_bullet(dataset)
    return {}


def track_command(args: Namespace) -> dict[str, Any]:
    import json

    # parse inputs
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
    truck.start_tracking_tables(track_datasets)

    return {}


def collect_command(args: Namespace) -> dict[str, Any]:
    print()
    return {}
