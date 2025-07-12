from __future__ import annotations

import typing

import absorb
from .. import cli_outputs

if typing.TYPE_CHECKING:
    from argparse import Namespace
    from typing import Any


def ls_command(args: Namespace) -> dict[str, Any]:
    # available datasets
    cli_outputs._print_title('Available datasets')
    if args.source is not None:
        sources = [args.source]
    else:
        sources = absorb.ops.get_sources()
    for source in sorted(sources):
        source_tables = absorb.ops.get_source_tables(source)
        if len(source_tables) > 0:
            cli_outputs._print_source_datasets_bullet(source, source_tables)

    # get tracked datasets
    tracked_datasets = absorb.ops.get_tracked_tables()
    if args.source is not None:
        tracked_datasets = [
            dataset
            for dataset in tracked_datasets
            if dataset['source_name'] == args.source
        ]
    tracked_datasets = sorted(
        tracked_datasets, key=lambda x: (x['source_name'], x['table_name'])
    )

    # print tracked datasets
    print()
    cli_outputs._print_title('Tracked datasets')
    if len(tracked_datasets) == 0:
        print('[none]')
    else:
        if args.verbose:
            _print_datasets_verbose(tracked_datasets)
        else:
            for dataset in tracked_datasets:
                cli_outputs._print_dataset_bullet(dataset)

    # get untracked collected datasets
    untracked_collected_datasets = absorb.ops.get_untracked_collected_tables(
        tracked_datasets=tracked_datasets
    )
    if args.source is not None:
        untracked_collected_datasets = [
            dataset
            for dataset in untracked_collected_datasets
            if dataset['source_name'] == args.source
        ]
    untracked_collected_datasets = sorted(
        untracked_collected_datasets,
        key=lambda x: (x['source_name'], x['table_name']),
    )

    # print untracked collected datasets
    if len(untracked_collected_datasets) > 0:
        print()
        cli_outputs._print_title('Untracked collected datasets')
        if args.verbose:
            _print_datasets_verbose(untracked_collected_datasets)
        else:
            for dataset in untracked_collected_datasets:
                cli_outputs._print_dataset_bullet(dataset)

    return {}


def _print_datasets_verbose(datasets: list[absorb.TrackedTable]) -> None:
    import toolstr

    rows = []
    for dataset in datasets:
        instance = absorb.Table.instantiate(dataset)
        available_range = instance.get_available_range()
        available_range_str = absorb.ops.format_coverage(
            available_range, instance.index_type
        )
        collected_range = instance.get_collected_range()
        if collected_range is not None:
            collected_range_str = absorb.ops.format_coverage(
                collected_range, instance.index_type
            )
        else:
            collected_range_str = '-'
        row = [
            dataset['source_name'],
            dataset['table_name'],
            available_range_str + '\n' + collected_range_str,
        ]
        rows.append(row)
    columns = ['source', 'table', 'available range\ncollected range']
    toolstr.print_multiline_table(rows, labels=columns)
