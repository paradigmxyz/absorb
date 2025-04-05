from __future__ import annotations

import typing

import tix
import truck

if typing.TYPE_CHECKING:
    from argparse import Namespace
    from typing import Any


from . import cli_parsing


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
                (['dataset'], {'nargs': '+'}),
                (['--path'], {}),
                (['--parameters'], {'nargs': '+'}),
            ],
        ),
    ]


def ls_command(args: Namespace) -> dict[str, Any]:
    import truck

    sources = truck.get_sources()
    source_tables = {
        source: truck.get_source_tables(source) for source in sources
    }
    print('Available datasets')
    for source in source_tables.keys():
        datasets = [cls.__name__ for cls in source_tables[source]]
        print('-', source + ':', ', '.join(datasets))
    print()
    print('Tracked Datasets')
    tracked_datasets = truck.get_tracked_tables()
    if len(tracked_datasets) == 0:
        print('[none]')
    else:
        for dataset in tracked_datasets:
            print(dataset)
    return {}


def track_command(args: Namespace) -> dict[str, Any]:
    parameters: dict[str, typing.Any] = {}
    for parameter in args.parameters:
        key, value = parameter.split('=')

    track_datasets: list[truck.TrackedTable] = []
    for dataset in args.dataset:
        if '.' in dataset:
            source, table = dataset.split('.')
        else:
            raise Exception()
        track_dataset: truck.TrackedTable = {
            'source_name': source,
            'table_name': table,
            'table_class': None,
            'parameters': parameters,
        }
        track_datasets.append(track_dataset)

    truck.create_tracked_tables(track_datasets)

    return {}


def collect_command(args: Namespace) -> dict[str, Any]:
    print()
    return {}
