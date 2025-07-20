from __future__ import annotations

import typing

import absorb
from .. import cli_outputs
from .. import cli_parsing

if typing.TYPE_CHECKING:
    from argparse import Namespace
    from typing import Any


def setup_command(args: Namespace) -> dict[str, Any]:
    if len(args.dataset) > 0:
        datasets = cli_parsing._parse_datasets(args)
    else:
        datasets = absorb.ops.get_tracked_tables()

    if args.regenerate_metadata:
        if len(datasets) == 1:
            word = 'dataset'
        else:
            word = 'datasets'
        print('generating metadata for', len(datasets), word)
        for dataset in datasets:
            instance = absorb.Table.instantiate(dataset)
            instance.setup_table_dir()

    if args.regenerate_config:
        old_config = absorb.ops.get_config()
        new_config = absorb.ops.get_default_config()
        for tracked_table in old_config['tracked_tables']:
            table = absorb.Table.instantiate(tracked_table)
            table_dict = table.create_table_dict()
            new_config['tracked_tables'].append(table_dict)
        absorb.ops.write_config(new_config)

    return {}
