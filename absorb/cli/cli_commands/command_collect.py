from __future__ import annotations

import typing

import absorb
from .. import cli_parsing

if typing.TYPE_CHECKING:
    from argparse import Namespace
    from typing import Any


def collect_command(args: Namespace) -> dict[str, Any]:
    import toolstr

    # parse which datasets to collect
    if len(args.dataset) > 0:
        datasets = cli_parsing._parse_datasets(args)
    else:
        datasets = [
            absorb.Table.instantiate(table)
            for table in absorb.ops.get_tracked_tables()
        ]

    # print datasets to collect
    if len(datasets) == 0:
        import sys

        print('specify one or more datasets to collect')
        sys.exit(0)
    elif len(datasets) > 1:
        toolstr.print_text_box(
            'Collecting multiple datasets',
            style='green',
            text_style='bold white',
        )
        for d, dataset in enumerate(datasets):
            name = dataset.full_name()
            absorb.ops.print_bullet(key=name, value=None, number=d + 1)
        print()

    # collect each dataset
    first = True
    for dataset in datasets:
        if not first:
            print()

        # instantiate dataset
        if dataset.write_range == 'append_only':
            data_ranges = cli_parsing._parse_ranges(args.range)
        else:
            data_ranges = None

        # collect dataset
        if args.setup_only:
            dataset.setup_table_dir()
        else:
            dataset.collect(
                data_range=data_ranges,
                dry=args.dry,
                overwrite=args.overwrite,
                verbose=args.verbose,
            )
        first = False

    return {}
