from __future__ import annotations

import typing

import truck

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
                        'nargs': '+',
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
    ]


def ls_command(args: Namespace) -> dict[str, Any]:
    import truck

    tracked_datasets = truck.get_tracked_tables()

    sources = truck.get_sources()
    source_tables = {
        source: truck.get_source_tables(source) for source in sources
    }
    print('Available datasets')
    for source in source_tables.keys():
        datasets = [cls.class_name(snake=True) for cls in source_tables[source]]
        print('-', source + ':', ', '.join(datasets))
    print()
    print('Tracked datasets')
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
        parameters[key] = value

    track_datasets: list[truck.TrackedTable] = []
    track_dataset: truck.TrackedTable
    for dataset in args.dataset:
        if '.' in dataset:
            source, table = dataset.split('.')
            track_dataset = {
                'source_name': source,
                'table_name': table,
                'table_class': None,
                'parameters': parameters,
            }
            track_datasets.append(track_dataset)
        else:
            for source_dataset in truck.get_source_tables(dataset):
                track_dataset = {
                    'source_name': dataset,
                    'table_name': source_dataset.__name__,
                    'table_class': None,
                    'parameters': parameters,
                }
                track_datasets.append(track_dataset)

    # use snake case throughout
    for track_dataset in track_datasets:
        track_dataset['source_name'] = truck.ops.names._camel_to_snake(
            track_dataset['source_name']
        )
        track_dataset['table_name'] = truck.ops.names._camel_to_snake(
            track_dataset['table_name']
        )

    if len(track_datasets) == 0:
        print('[no datasets to track]')
    else:
        print('Now tracking these datasets')
        for track_dataset in track_datasets:
            print(
                '- '
                + track_dataset['source_name']
                + '.'
                + track_dataset['table_name']
            )
    truck.create_tracked_tables(track_datasets)

    return {}


def collect_command(args: Namespace) -> dict[str, Any]:
    print()
    return {}
