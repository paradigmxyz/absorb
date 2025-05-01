from __future__ import annotations

import os
import typing
import truck

from . import names

if typing.TYPE_CHECKING:
    import types


def get_source_module(source: str) -> types.ModuleType:
    import importlib

    return importlib.import_module(
        'truck.datasets.' + names._camel_to_snake(source)
    )


def get_table_class(source: str, table_name: str) -> type[truck.Table]:
    return getattr(get_source_module(source), names._snake_to_camel(table_name))  # type: ignore


def get_sources(*, snake: bool = True) -> list[str]:
    import truck.datasets

    sources = [
        filename.rsplit('.py', maxsplit=1)[0]
        for filename in os.listdir(truck.datasets.__path__[0])
        if not filename.startswith('__')
    ]

    if not snake:
        sources = [names._snake_to_camel(source) for source in sources]

    return sources


def get_source_tables(source: str) -> list[type[truck.Table]]:
    module = get_source_module(source)
    if hasattr(module, 'get_tables'):
        return module.get_tables()  # type: ignore
    else:
        return [
            value
            for key, value in vars(module).items()
            if isinstance(value, type) and issubclass(value, truck.Table)
        ]


def resolve_table(
    reference: truck.TableReference,
    *,
    parameters: dict[str, typing.Any] | None = None,
) -> truck.Table:
    if isinstance(reference, truck.Table):
        return reference

    elif isinstance(reference, dict):
        return truck.Table.instantiate(reference)

    elif isinstance(reference, str):
        source, table = reference.split('.')
        camel_table = names._snake_to_camel(table)
        snake_table = names._camel_to_snake(table)
        if parameters is None:
            parameters = {}
        tracked_table: truck.TrackedTable = {
            'source_name': source,
            'table_name': snake_table,
            'table_class': 'truck.datasets.' + source + '.' + camel_table,
            'parameters': parameters,
        }
        return truck.Table.instantiate(tracked_table)

    else:
        raise Exception()


def get_available_tables() -> list[truck.TrackedTable]:
    tables = []
    for source in get_sources():
        for table in get_source_tables(source):
            if len(table.parameter_types) == 0:
                snake_table = truck.ops.names._camel_to_snake(table.__name__)
                tracked_table: truck.TrackedTable = {
                    'source_name': source,
                    'table_name': snake_table,
                    'table_class': 'truck.datasets.'
                    + source
                    + '.'
                    + table.__name__,
                    'parameters': {},
                }
                tables.append(tracked_table)
    return tables
