from __future__ import annotations

import typing

import tix
import truck

if typing.TYPE_CHECKING:
    from argparse import Namespace
    from typing import Any


from . import cli_parsing


def get_subcommands() -> (
    list[tuple[str, str, typing.Callable[[Namespace], dict[str, Any]]]]
):
    return [
        (
            'ls',
            'list tracked datasets',
            ls_command,
        ),
        (
            'collect',
            'collect datasets',
            collect_command,
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
    print('[none]')
    return {}


def collect_command(args: Namespace) -> dict[str, Any]:
    print()
    return {}
