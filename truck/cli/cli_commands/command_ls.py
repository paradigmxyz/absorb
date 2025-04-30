from __future__ import annotations

import typing

import truck
from .. import cli_outputs

if typing.TYPE_CHECKING:
    from argparse import Namespace
    from typing import Any


def ls_command(args: Namespace) -> dict[str, Any]:
    import toolstr

    tracked_datasets = truck.get_tracked_tables()

    # available datasets
    cli_outputs._print_title('Available datasets')
    for source in sorted(truck.get_sources()):
        cli_outputs._print_source_datasets_bullet(
            source, truck.get_source_tables(source)
        )

    # tracked datasets
    print()
    if args.verbose:
        cli_outputs._print_title('Tracked datasets')
        if len(tracked_datasets) == 0:
            print('[none]')
        else:
            rows = []
            for dataset in tracked_datasets:
                instance = truck.Table.instantiate(dataset)
                available_range = instance.get_available_range()
                available_range = truck.ops.format_range(available_range)
                collected_range = instance.get_collected_range()
                if collected_range is not None:
                    collected_range = truck.ops.format_range(collected_range)
                else:
                    collected_range = '-'
                row = [
                    dataset['source_name'],
                    dataset['table_name'],
                    available_range + '\n' + collected_range,
                ]
                rows.append(row)
            columns = ['source', 'table', 'available range\ncollected range']
            toolstr.print_multiline_table(rows, labels=columns)
    else:
        cli_outputs._print_title('Tracked datasets')
        if len(tracked_datasets) == 0:
            print('[none]')
        else:
            for dataset in tracked_datasets:
                cli_outputs._print_dataset_bullet(dataset)

    return {}
