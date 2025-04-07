from __future__ import annotations

import typing

import truck

if typing.TYPE_CHECKING:
    from argparse import Namespace
    from typing import Any


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
