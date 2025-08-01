from __future__ import annotations

import typing

import absorb
from .. import cli_parsing

if typing.TYPE_CHECKING:
    from argparse import Namespace
    from typing import Any


def path_command(args: Namespace) -> dict[str, Any]:
    path = _get_path(args)
    print(path)
    return {}


def _get_path(args: Namespace) -> str:
    if args.dataset is None:
        return absorb.ops.get_absorb_root(warn=False)
    elif args.glob:
        tracked_dataset = cli_parsing._parse_datasets(args)[0]
        instance = absorb.Table.instantiate(tracked_dataset)
        return instance.get_data_glob(warn=False)
    elif '.' in args.dataset:
        source, table = args.dataset.split('.')
        return absorb.ops.get_table_dir(source=source, table=table, warn=False)
    else:
        return absorb.ops.get_source_dir(args.dataset, warn=False)
